/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);

    private static final Request REQUEST_OF_DEATH = Request.requestOfDeath; //结束请求标识

    /** The number of log entries to log before starting a snapshot */
    private static int snapCount = ZooKeeperServer.getSnapCount(); // 快照个数

    /**
     * The total size of log entries before starting a snapshot
     */
    private static long snapSizeInBytes = ZooKeeperServer.getSnapSizeInBytes(); //快照大小

    /**
     * Random numbers used to vary snapshot timing
     */
    private int randRoll;
    private long randSize;

    private final BlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>(); //待处理的请求队列

    private final Semaphore snapThreadMutex = new Semaphore(1);//快照线程互斥信号量

    private final ZooKeeperServer zks;

    private final RequestProcessor nextProcessor; //下一个处理器

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     * 阅读此注释
     */
    private final Queue<Request> toFlush; //翻译：已写入并等待刷新到磁盘的事务请求
    private long lastFlushTime;

    public SyncRequestProcessor(ZooKeeperServer zks, RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks.getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;// FinalRequestProcessor
        this.toFlush = new ArrayDeque<>(zks.getMaxBatchSize());
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    private long getRemainingDelay() {
        long flushDelay = zks.getFlushDelay();
        long duration = Time.currentElapsedTime() - lastFlushTime;
        if (duration < flushDelay) {
            return flushDelay - duration;
        }
        return 0;
    }

    /** If both flushDelay and maxMaxBatchSize are set (bigger than 0), flush
     * whenever either condition is hit. If only one or the other is
     * set, flush only when the relevant condition is hit.
     * 如果同时设置了flushDelay和MaxBatchSize(大于0)，则在遇到任何一个条件时刷新。如果只设置一个或另一个，则仅在达到相关条件时刷新。
     */
    private boolean shouldFlush() {
        long flushDelay = zks.getFlushDelay();
        long maxBatchSize = zks.getMaxBatchSize();
        if ((flushDelay > 0) && (getRemainingDelay() == 0)) {
            return true;
        }
        return (maxBatchSize > 0) && (toFlush.size() >= maxBatchSize);
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param size
     */
    public static void setSnapSizeInBytes(long size) {
        snapSizeInBytes = size;
    }

    private boolean shouldSnapshot() {
        int logCount = zks.getZKDatabase().getTxnCount();
        long logSize = zks.getZKDatabase().getTxnSize();
        return (logCount > (snapCount / 2 + randRoll))
               || (snapSizeInBytes > 0 && logSize > (snapSizeInBytes / 2 + randSize));
    }

    private void resetSnapshotStats() {
        randRoll = ThreadLocalRandom.current().nextInt(snapCount / 2);
        randSize = Math.abs(ThreadLocalRandom.current().nextLong() % (snapSizeInBytes / 2));
    }

    /**
     * SyncReqeustProcessor负责对某个请求进行持久化(事务日志)，但是它又不仅仅只对Request进行持久化，还会打快照（对DataTree进行持久化）
     * 在SyncReqeustProcessor中，不是每接收到一个Reqeust就直接进行持久化，它的思路是批量持久化
     */
    @Override
    public void run() {
        /**
         * ZooKeeper使用事务日志和快照来持久化每个事务(注意是日志先写)。
         * 属性：snapCount 指定ZooKeeper在将内存数据库序列化为快照之前，需要先写多少次事务日志。也就是说，每写几次事务日志，就快照一次
         */
        try {
            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            resetSnapshotStats();//重置日志状态 确保集合中的所有服务器不会在同一时间进行快照
            lastFlushTime = Time.currentElapsedTime();//上一次flush的时间
            while (true) {
                ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUE_SIZE.add(queuedRequests.size());

                long pollTime = Math.min(zks.getMaxWriteQueuePollTime(), getRemainingDelay());
                /**
                 * 从队列获取事务请求Request
                 */
                Request si = queuedRequests.poll(pollTime, TimeUnit.MILLISECONDS);
                if (si == null) {
                    /* We timed out looking for more writes to batch, go ahead and flush immediately */
                    flush();//没有请求,看是否有数据进行进行刷盘处理(之前未达到刷盘要求的请求会等待,满足要求后批量刷盘)
                    si = queuedRequests.take();//阻塞等待下一个请求到来
                }

                if (si == REQUEST_OF_DEATH) {
                    break;
                }

                long startProcessTime = Time.currentElapsedTime();
                ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUE_TIME.add(startProcessTime - si.syncQueueStartTime);

                // track the number of records written to the log 跟踪写入日志的记录数量
                /**
                 *  zks.getZKDatabase().append(si) 添加一个事务日志记录；只是添加到缓冲区中；读请求或节流请求不会真正添加事务日志
                 *  1、ZKDatabase---->FileTxnSnapLog----->FileTxnLog:写入到 FileOutputStream
                 */
                if (!si.isThrottled() && zks.getZKDatabase().append(si)) {
                    if (shouldSnapshot()) {
                        resetSnapshotStats();
                        // roll the log
                        zks.getZKDatabase().rollLog();
                        // take a snapshot
                        if (!snapThreadMutex.tryAcquire()) {
                            LOG.warn("Too busy to snap, skipping");
                        } else {
                            /**
                             * 开启一个数据快照线程
                             */
                            new ZooKeeperThread("Snapshot Thread") {
                                public void run() {
                                    try {
                                        /**
                                         * 对内存数据进行快照
                                         */
                                        zks.takeSnapshot();
                                    } catch (Exception e) {
                                        LOG.warn("Unexpected exception", e);
                                    } finally {
                                        snapThreadMutex.release();
                                    }
                                }
                            }.start();
                        }
                    }
                } else if (toFlush.isEmpty()) {
                    // optimization for read heavy workloads
                    // iff this is a read or a throttled request(which doesn't need to be written to the disk),
                    // and there are no pending flushes (writes), then just pass this to the next processor
                    /**
                     * 来自翻译：
                     * 如果这是一个读或节流请求(不需要写入磁盘)，并且没有挂起的刷新(写入)，那么只需将其传递给下一个处理器
                     */
                    if (nextProcessor != null) {
                        nextProcessor.processRequest(si);
                        if (nextProcessor instanceof Flushable) {
                            ((Flushable) nextProcessor).flush();
                        }
                    }
                    continue;
                }
                /**
                 * 将当前请求添加到待刷盘的事务队列中，toFlush是Queue,存的是已写入并等待刷新到磁盘的事务请求对象
                 *
                 */
                toFlush.add(si);
                /**
                 * shouldFlush表明不是每次请求都持久化一次，而是批量持久化
                 */
                if (shouldFlush()) {
                    /**
                     * flush 事务 并进入下一个处理器
                     */
                    flush();
                }
                ServerMetrics.getMetrics().SYNC_PROCESS_TIME.add(Time.currentElapsedTime() - startProcessTime);
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    private void flush() throws IOException, RequestProcessorException {
        if (this.toFlush.isEmpty()) {
            return;
        }

        ServerMetrics.getMetrics().BATCH_SIZE.add(toFlush.size());

        long flushStartTime = Time.currentElapsedTime();
        /**
         * 提交事务底层日志数据;数据刷盘
         */
        zks.getZKDatabase().commit();
        ServerMetrics.getMetrics().SYNC_PROCESSOR_FLUSH_TIME.add(Time.currentElapsedTime() - flushStartTime);

        if (this.nextProcessor == null) {
            this.toFlush.clear();
        } else {
            while (!this.toFlush.isEmpty()) {
                //前面已经将请求事务日志做了持久化，从toFlush中移除该请求，交给下一个处理器处理
                final Request i = this.toFlush.remove();
                long latency = Time.currentElapsedTime() - i.syncQueueStartTime;
                ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUE_AND_FLUSH_TIME.add(latency);
                /**
                 * 将request交给下一个处理器
                 * 这里nextProcessor是FinalRequestProcessor
                 */
                this.nextProcessor.processRequest(i);
            }
            if (this.nextProcessor instanceof Flushable) {
                ((Flushable) this.nextProcessor).flush();
            }
        }
        lastFlushTime = Time.currentElapsedTime();
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(REQUEST_OF_DEATH);
        try {
            this.join();
            this.flush();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while wating for {} to finish", this);
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(final Request request) {
        Objects.requireNonNull(request, "Request cannot be null");

        request.syncQueueStartTime = Time.currentElapsedTime();
        /**
         * 将请求添加到SyncRequestProcessor的队列中
         */
        queuedRequests.add(request);
        ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUED.add(1);
    }

}
