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

@RestController
public class Lock0321 {

    private  final static Logger log = LoggerFactory.getLogger(Lock0321.class);


    @Autowired
    RedisTemplate<String,String> redisTemplate;

    String maotai = "maotai20210321001";//茅台商品编号

    @PostConstruct
    public void init(){
        //此处模拟向缓存中存入商品库存操作
        redisTemplate.opsForValue().set(maotai,"100");
    }

    /**
     * 未加锁
     * @return
     */
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
            Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai)); // 1
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
     * 现象：本地锁在多节点下失效(集群/分布式)
     * 原因：本地锁它只能锁住本地JVM进程中的多个线程，对于多个JVM进程的不同线程间是锁不住的
     *
     * 怎么办？
     *  我们需要一种分布式锁，期望能在分布式环境下提供锁服务，并且达到本地锁的效果：不仅能锁住同一jvm进程下的不同线程，更要能锁住不同jvm进程下的不同线程
     *
     *  为什么需要分布式锁：
     *  1：为了效率：防止不同节点之间做相同的事情，浪费资源 （定时任务）
     *  2：为了安全：有些事情在同一时间只允许一个线程去做
     *
     *  分布式锁的特点：
     *  1，互斥性：不仅要在同一jvm进程下的不同线程间互斥，更要在不同jvm进程下的不同线程间互斥
     *  2，锁超时：支持锁的自动释放，防止死锁
     *  3，正确，高效，高可用：解铃还须系铃人（加锁和解锁必须是同一个线程），加锁和解锁操作一定要高效，提供锁的服务要具备容错性
     *  4，可重入：如果一个线程拿到了锁之后继续去获取锁还能获取到，我们称锁是可重入的（方法的递归调用）
     *  5，阻塞/非阻塞：如果获取不到直接返回视为非阻塞的，如果获取不到会等待锁的释放直到获取锁或者等待超时，视为阻塞的
     *  6，公平/非公平：按照请求的顺序获取锁视为公平的
     *
     *  要基于redis实现分布式锁：
     *    setnx 结合 expire
     *
     *      if (setnx(key,value) == 1) {
     *          expire(key,30);
     *          try{
     *              //业务操作
     *          }finally{
     *              //释放锁
     *              del key;
     *          }
     *      }
     *
     */


    String lockey = "maotailock1";

    @GetMapping("/get/maotai3")
    public String seckillMaotai3() {
        Boolean islock = redisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                redisConnection.setNX(lockey.getBytes(),"1".getBytes());
                return null;
            }
        });
        //获取锁
       /* Boolean islock = redisTemplate.opsForValue().setIfAbsent(lockey, "1");*/
        if (islock) {
            //设置锁的过期时间
            redisTemplate.expire(lockey,5, TimeUnit.SECONDS);
            try {
                Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai)); // 1
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
     * 问题：
     *  1，setnx 和 expire是非原子性操作
     *   有两种解决方案：
     *    1，2.6以前可用使用lua脚本
     *    2，2.6以后可用set命令 set a b nx ex
     *
     */


    @GetMapping("/get/maotai4")
    public String seckillMaotai4() {

        //获取锁
        String locklua ="" +
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
                        "1".getBytes(),
                        "5".getBytes()
                );
                return eval;
            }
        });

        if (islock) {
            try {
                Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai)); // 1
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

                redisTemplate.delete(lockey);
            }
        }
        return "dont get lock";
    }


    /**
     * setnx  k v   ex设置过期时间
     * @return
     */
    @GetMapping("/get/maotai4_2")
    public String seckillMaotai4_2() {

        //获取锁
        Boolean islock = redisTemplate.opsForValue().setIfAbsent(lockey,"1",5,TimeUnit.SECONDS);
        if (islock) {
            try {
                Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai)); // 1
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

                redisTemplate.delete(lockey);
            }
        }
        return "dont get lock";
    }


    /**
     *      *   错误解锁：
     *      *    如何保证解铃还须系铃人：给锁加一个唯一标识
     * @return
     */
    @GetMapping("/get/maotai4_3")
    public String seckillMaotai4_3() {
        // 声明 requestid
        String requestid = UUID.randomUUID().toString() + Thread.currentThread().getId();

        //获取锁
        Boolean islock = redisTemplate.opsForValue().setIfAbsent(lockey,requestid,5,TimeUnit.SECONDS);
        if (islock) {
            try {
                Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai)); // 1
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
                //判断是自己的锁才能去释放
                String id = redisTemplate.opsForValue().get(lockey);
                    if (id !=null && id.equals(requestid)) {
                        redisTemplate.delete(lockey);
                }
            }
        }
        return "dont get lock";
    }


    @GetMapping("/get/maotai4_4")
    public String seckillMaotai4_4() {
        String requestId = UUID.randomUUID().toString() + Thread.currentThread().getId();
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
     * 3,锁续期/锁续命
     *  拿到锁之后执行业务，业务的执行时间超过了锁的过期时间
     *
     *  如何做？
     *  给拿到锁的线程创建一个守护线程(看门狗)，守护线程定时/延迟 判断拿到锁的线程是否还继续持有锁，如果持有则为其续期
     *
     */
    //模拟一下守护线程为其续期
    ScheduledExecutorService executorService;//创建守护线程池
    ConcurrentSkipListSet<String> set = new ConcurrentSkipListSet<String>();//队列

    @PostConstruct
    public void init2(){
        executorService = Executors.newScheduledThreadPool(1);

        //编写续期的lua
        String expirrenew = "" +
                "if redis.call('get',KEYS[1]) == ARGV[1] then redis.call('expire',KEYS[1],ARGV[2]) ; return true " +
                "else return false " +
                "end";

        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Iterator<String> iterator = set.iterator();
                while (iterator.hasNext()) {
                    String rquestid = iterator.next();

                    redisTemplate.execute(new RedisCallback<Boolean>() {
                        @Override
                        public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                            Boolean eval = false;
                            try {
                                eval = redisConnection.eval(
                                        expirrenew.getBytes(),
                                        ReturnType.BOOLEAN,
                                        1,
                                        lockey.getBytes(),
                                        rquestid.getBytes(),
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
        String requestid = UUID.randomUUID().toString() + Thread.currentThread().getId();
        //获取锁
        Boolean islock = redisTemplate.opsForValue().setIfAbsent(lockey,requestid,5,TimeUnit.SECONDS);
        if (islock) {
            //获取锁成功后让守护线程为其续期
            set.add(requestid);
            try {
                Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai)); // 1
                //如果还有库存
                if (count > 0) {
                    //抢到了茅台，库存减一
                    redisTemplate.opsForValue().set(maotai,String.valueOf(count-1));
                    //后续操作 do something
                    seckillMaotai5();
                    //模拟业务超时
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
               set.remove(requestid);
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
                                requestid.getBytes()
                        );
                        return eval;
                    }
                });
            }
        }
        return "dont get lock";
    }


    /**
     *
     * 4,如何支持可重入()  重入次数
     *   重入次数/过期时间
     *    获取锁
     *      获取锁
     *        获取锁
     *        释放锁   结论：锁的获取和释放一定是成对出现的！ set lockey requestid del lockey
     *       释放锁
     *    释放锁
     *
     *   基于本地实现
     *   还是基于redis 但是更换了数据类型，采用hash类型来实现
     *    key   field  value
     *    锁名称 requestid  重入次数
     *
     *   5,阻塞/非阻塞的问题：现在的锁是非阻塞的，一旦获取不到锁直接返回了
     *   如何做一个阻塞锁呢？
     *    获取不到就等待锁的释放，直到获取到锁或者等待超时
     *    1：基于客户端轮询的方案
     *    2：基于redis的发布/订阅方案
     *
     *
     *    有没有好的实现呢？
     *    Redisson
     *
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
        //要去获取锁
        RLock lock = redissonClient.getLock(lockey);
        lock.lock();
        try {
            Integer count = Integer.parseInt(redisTemplate.opsForValue().get(maotai)); // 1
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
        return "";
    }

    /**
     *
     * Redisson的源码剖析
     * 1 ，加锁的（是否支持重入）
     * 2，锁续期的
     * 3，阻塞获取
     * 4，释放
     *
     *  redisson_lock__channel:{maotailock}
     *
     */
    /**
     * 源码如下：
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
