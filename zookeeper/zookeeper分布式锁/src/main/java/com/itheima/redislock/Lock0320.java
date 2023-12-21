package com.itheima.redislock;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

//@RestController
public class Lock0320 {
    private  final static Logger log = LoggerFactory.getLogger(Lock0320.class);


    @Autowired
    RedisTemplate<String,String> redisTemplate;

    String maotai = "maotai20210320001";//茅台商品编号

    @PostConstruct
    public void init(){
        //此处模拟向缓存中存入商品库存操作
        redisTemplate.opsForValue().set(maotai,"100");
    }


    @GetMapping("/get/maotai")
    public String seckillMaotai() {

        Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai));
        //如果还有库存
        if (count > 0) {
            //抢到了茅台，库存减一
            redisTemplate.opsForValue().set(maotai,String.valueOf(count-1));
            //后续操作 do something
            log.info("我抢到茅台了!");
            return "ok";
        }else {
            return "no";
        }
    }

    @GetMapping("/get/maotai2")
    public String seckillMaotai2() {
        synchronized (this) {
            Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai));
            //如果还有库存
            if (count > 0) {
                //抢到了茅台，库存减一
                redisTemplate.opsForValue().set(maotai,String.valueOf(count-1));
                //后续操作 do something
                log.info("我抢到茅台了!");
                return "ok";
            }else {
                return "no";
            }
        }
    }

    /**
     * 问题：
     *  1：本地锁只能锁住本地JVM进程中的多个线程，对于多个JVM进程间的多个线程是锁不住的
     *
     *  解决：需要分布式锁，来满足在分布式环境下达到本地锁的效果
     *
     *  为何要分布式锁：
     *  1：为了效率：避免不同节点间做重复的工作而浪费资源
     *  2：为了正确性：有些事情同一时刻只能一个人做
     *
     *  分布式锁的特性：
     *  1，互斥性：除了同一节点内的不同线程要互斥外，更重要的是在不同节点的不同线程间互斥
     *  2，锁超时：支持锁自动释放，防止死锁
     *  3，正确，高效，高可用：解铃还须系铃人（加锁和解锁必须是同一个请求线程），加锁和解锁操作必须高效，提供锁的服务要能容错
     *  4，可重入：一个线程获取锁后还能继续获取锁那就是可重入的
     *  5，阻塞/非阻塞：如果获取不到锁立即返回视为非阻塞的，如果获取不到锁时可以等待直到获取锁视为阻塞的
     *  6，公平/非公平：公平就是按请求加锁的顺序获得锁
     *
     *
     *  基于redis的分布式锁
     *   setnx + expire
     *      if (setnx(key,value) == 1) {
     *          expire(key,30);
     *          try{
     *              //业务处理
     *          }finally{
     *              //释放锁
     *              del key
     *          }
     *      }
     */

    String lockey = "maotailock";
    @GetMapping("/get/maotai3")
    public String seckillMaotai3() {
        /*redisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                Boolean setNX = redisConnection.setNX(lockey.getBytes(), "1".getBytes());
                return setNX;
            }
        });*/
        Boolean islock = redisTemplate.opsForValue().setIfAbsent(lockey, "1");
        if (islock) {
            redisTemplate.expire(lockey,5, TimeUnit.SECONDS);
            try {
                Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai));
                //如果还有库存
                if (count > 0) {
                    //抢到了茅台，库存减一
                    redisTemplate.opsForValue().set(maotai,String.valueOf(count-1));
                    //后续操作 do something
                    log.info("我抢到茅台了!");
                    return "ok";
                }else {
                    return "no";
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                //释放锁
                redisTemplate.delete(lockey);
            }
        }
        return "dont get lock";
    }


    /**
     *  问题：
     *  1:setnx 和 expire 是非原子操作
     *
     *   两种做法：
     *   1，2.6以前借助lua
     *   2，2.6以后使用set命令
     *
     *   2:错误释放锁
     */

    @GetMapping("/get/maotai4")
    public String seckillMaotai4() {
        String requestId = UUID.randomUUID().toString() + Thread.currentThread().getId();
       /* String locklua = "" +
                "if redis.call('setnx',KEYS[1],ARGV[1]) == 1 then redis.call('expire',KEYS[1],ARGV[2]) ; return true " +
                "else return false " +
                "end";
        Boolean islock = redisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                Boolean eval = redisConnection.eval(
                        locklua.getBytes(),
                        ReturnType.BOOLEAN,
                        1,
                        lockey.getBytes(),
                        requestId.getBytes(),
                        "5".getBytes()
                );
                return eval;
            }
        });*/

        Boolean islock = redisTemplate.opsForValue().setIfAbsent(lockey, requestId,5,TimeUnit.SECONDS);
        if (islock) {
            try {
                Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai));
                //如果还有库存
                if (count > 0) {
                    //抢到了茅台，库存减一
                    redisTemplate.opsForValue().set(maotai,String.valueOf(count-1));
                    //后续操作 do something
                    log.info("我抢到茅台了!");
                    return "ok";
                }else {
                    return "no";
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                //释放锁
                //这种做法也不是原子操作
                /*String id = redisTemplate.opsForValue().get(lockey);
                if (id!=null && id.equals(requestId)) {
                    redisTemplate.delete(lockey);
                }*/
                String unlocklua = "" +
                        "if redis.call('get',KEYS[1]) == ARGV[1] then redis.call('del',KEYS[1]) ; return true " +
                        "else return false " +
                        "end";
                redisTemplate.execute(new RedisCallback<Boolean>() {
                    @Override
                    public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                        Boolean eval = redisConnection.eval(
                                unlocklua.getBytes(),
                                ReturnType.BOOLEAN,
                                1,
                                lockey.getBytes(),
                                requestId.getBytes()
                        );
                        return eval;
                    }
                });
            }
        }
        return "dont get lock";
    }

    /**
     *  3,锁续期：守护线程(看门狗)
     *  模拟一下
     */
    ScheduledExecutorService executorService;
    ConcurrentSkipListSet<String> set = new ConcurrentSkipListSet<String>();

    @PostConstruct
    public void init2() {
        executorService = Executors.newScheduledThreadPool(1);
        String renewExpirationLua = "" +
                "if redis.call('get',KEYS[1]) == ARGV[1] then redis.call('expire',KEYS[1],ARGV[2]); return true " +
                "else return false " +
                "end";
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Iterator<String> iterator = set.iterator();
                while (iterator.hasNext()) {
                    String requestid = iterator.next();
                    redisTemplate.execute(new RedisCallback<Boolean>() {
                        @Override
                        public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                            Boolean eval = false;
                            try {
                                eval = redisConnection.eval(
                                        renewExpirationLua.getBytes(),
                                        ReturnType.BOOLEAN,
                                        1,
                                        lockey.getBytes(),
                                        requestid.getBytes(),
                                        "5".getBytes()
                                );
                            } catch (Exception e) {
                                log.error("锁续期失败,{}",e.getMessage());
                            }
                            return eval;
                        }
                    });
                }
            }
        },0,1,TimeUnit.SECONDS);
    }

    @GetMapping("/get/maotai5")
    public String seckillMaotai5() {
        String requestId = UUID.randomUUID().toString() + Thread.currentThread().getId();
        Boolean islock = redisTemplate.opsForValue().setIfAbsent(lockey, requestId,5,TimeUnit.SECONDS);
        if (islock) {
            //让守护线程为其续期
            set.add(requestId);
            try {
                Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai));
                //如果还有库存
                if (count > 0) {
                    //抢到了茅台，库存减一
                    redisTemplate.opsForValue().set(maotai,String.valueOf(count-1));
                    //后续操作 do something
                    TimeUnit.SECONDS.sleep(10);
                    log.info("我抢到茅台了!");
                    return "ok";
                }else {
                    return "no";
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                //解除锁续期
                set.remove(requestId);
                //释放锁
                String unlocklua = "" +
                        "if redis.call('get',KEYS[1]) == ARGV[1] then redis.call('del',KEYS[1]) ; return true " +
                        "else return false " +
                        "end";
                redisTemplate.execute(new RedisCallback<Boolean>() {
                    @Override
                    public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                        Boolean eval = redisConnection.eval(
                                unlocklua.getBytes(),
                                ReturnType.BOOLEAN,
                                1,
                                lockey.getBytes(),
                                requestId.getBytes()
                        );
                        return eval;
                    }
                });
            }
        }
        return "dont get lock";
    }


    /**
     * 4,锁重入====>采用redis的hash类型
     *
     * key field value
     * 锁名 请求id 重入次数
     *
     * 5，阻塞/非阻塞：目前是非阻塞的
     * 阻塞锁的实现方案：
     *  1：客户端轮询
     *  2：基于redis的发布/订阅
     */

    /**
     * 好的解决方案：Redisson
     */
    @Value("${spring.redis.host}")
    String host;
    @Value("${spring.redis.port}")
    String port;
    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://"+host+":"+port);
        return Redisson.create(config);
    }

    @Autowired
    RedissonClient redissonClient;

    @GetMapping("/get/maotai6")
    public String seckillMaotai6() {
        RLock lock = redissonClient.getLock(lockey);
        //获取锁
        lock.lock();

        try {
            Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai));
            //如果还有库存
            if (count > 0) {
                //抢到了茅台，库存减一
                redisTemplate.opsForValue().set(maotai,String.valueOf(count-1));
                //后续操作 do something
                log.info("我抢到茅台了!");
                return "ok";
            }else {
                return "no";
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        return "dont get lock";
    }

    /**
     * 原理
     * 1，加锁
     * <T> RFuture<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
     *         internalLockLeaseTime = unit.toMillis(leaseTime);
     *
     *         return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
     *                    #如果锁key不存在
     *                   "if (redis.call('exists', KEYS[1]) == 0) then " +
     *                        #设置锁key,field是唯一标识,value是重入次数
     *                       "redis.call('hset', KEYS[1], ARGV[2], 1); " +
     *                       #设置锁key的过期时间 默认30s
     *                       "redis.call('pexpire', KEYS[1], ARGV[1]); " +
     *                       "return nil; " +
     *                   "end; " +
     *                   #如果锁key存在
     *                   "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
     *                        #重入次数+1
     *                       "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
     *                        #重置过期时间
     *                       "redis.call('pexpire', KEYS[1], ARGV[1]); " +
     *                       "return nil; " +
     *                   "end; " +
     *                   "return redis.call('pttl', KEYS[1]);",
     *                     Collections.<Object>singletonList(getName()), internalLockLeaseTime, getLockName(threadId));
     *     }
     *
     *  2，锁续期
     *   private void scheduleExpirationRenewal(final long threadId) {
     *         if (expirationRenewalMap.containsKey(getEntryName())) {
     *             return;
     *         }
     *
     *         Timeout task = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
     *             @Override
     *             public void run(Timeout timeout) throws Exception {
     *                 //续期函数的真正实现
     *                 RFuture<Boolean> future = renewExpirationAsync(threadId);
     *
     *                 future.addListener(new FutureListener<Boolean>() {
     *                     @Override
     *                     public void operationComplete(Future<Boolean> future) throws Exception {
     *                         expirationRenewalMap.remove(getEntryName());
     *                         if (!future.isSuccess()) {
     *                             log.error("Can't update lock " + getName() + " expiration", future.cause());
     *                             return;
     *                         }
     *
     *                         if (future.getNow()) {
     *                             // reschedule itself  再次调用自己,最终形成的结果就是每隔10秒续期一次
     *                             scheduleExpirationRenewal(threadId);
     *                         }
     *                     }
     *                 });
     *             }
     *          // internalLockLeaseTime=30 * 1000 即30秒
     *         }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS); //30/3=10秒后异步执行续期函数
     *
     *         if (expirationRenewalMap.putIfAbsent(getEntryName(), new ExpirationEntry(threadId, task)) != null) {
     *             task.cancel();
     *         }
     *     }
     *
     *     续期的lua脚本:判断key，field存在则重置过期时间
     *     protected RFuture<Boolean> renewExpirationAsync(long threadId) {
     *         return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
     *                 "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
     *                     "redis.call('pexpire', KEYS[1], ARGV[1]); " +
     *                     "return 1; " +
     *                 "end; " +
     *                 "return 0;",
     *             Collections.<Object>singletonList(getName()),
     *             internalLockLeaseTime, getLockName(threadId));
     *     }
     *
     *
     *
     * 4，阻塞锁实现
     *  public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
     *         long threadId = Thread.currentThread().getId();
     *         Long ttl = tryAcquire(leaseTime, unit, threadId);
     *         // lock acquired
     *         if (ttl == null) {
     *             return;
     *         }
     *         //如果没有获取到锁，则订阅：redisson_lock__channel:{key} 频道
     *         RFuture<RedissonLockEntry> future = subscribe(threadId);
     *         commandExecutor.syncSubscription(future);
     *
     *         try {
     *             while (true) {
     *                //尝试再获取一次
     *                 ttl = tryAcquire(leaseTime, unit, threadId);
     *                 // lock acquired
     *                 if (ttl == null) {
     *                     break;
     *                 }
     *
     *                 // waiting for message 阻塞等待锁订阅频道的消息,一旦锁被释放,就会得到信号通知,继续尝试获取锁
     *                 if (ttl >= 0) {
     *                     getEntry(threadId).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
     *                 } else {
     *                     getEntry(threadId).getLatch().acquire();
     *                 }
     *             }
     *         } finally {
     *            //获取到锁后取消订阅
     *             unsubscribe(future, threadId);
     *         }
     * //        get(lockAsync(leaseTime, unit));
     *     }
     *
     *
     * 5，解锁
     * protected RFuture<Boolean> unlockInnerAsync(long threadId) {
     *         return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
     *                 //key已经不存在了，则向redisson_lock__channel:{key}频道发布锁释放消息
     *                 "if (redis.call('exists', KEYS[1]) == 0) then " +
     *                     "redis.call('publish', KEYS[2], ARGV[1]); " +
     *                     "return 1; " +
     *                 "end;" +
     *                  // hash 中的field 不存在时直接返回,
     *                 "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
     *                     "return nil;" +
     *                 "end; " +
     *                 "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
     *                 //重入次数-1后如果还大于0,延长过期时间
     *                 "if (counter > 0) then " +
     *                     "redis.call('pexpire', KEYS[1], ARGV[2]); " +
     *                     "return 0; " +
     *                 "else " +
     *                 //重入次数-1后如果归0,则删除key，并向redisson_lock__channel:{key}频道发布锁释放消息
     *                     "redis.call('del', KEYS[1]); " +
     *                     "redis.call('publish', KEYS[2], ARGV[1]); " +
     *                     "return 1; "+
     *                 "end; " +
     *                 "return nil;",
     *                 Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.unlockMessage, internalLockLeaseTime, getLockName(threadId));
     *
     *     }
     */
}
