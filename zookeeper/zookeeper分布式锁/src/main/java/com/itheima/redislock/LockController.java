package com.itheima.redislock;
import org.redisson.Redisson;
import org.redisson.RedissonRedLock;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

// @RestController
public class LockController {
    private  final static Logger log = LoggerFactory.getLogger(LockController.class);

    @Autowired
    RedisTemplate<String,String> redisTemplate;

    String key = "maotai20210319001";//茅台商品编号

    ScheduledExecutorService executorService;
    ConcurrentSkipListSet<String> set = new ConcurrentSkipListSet<>();
    @PostConstruct
    void init() {
        redisTemplate.opsForValue().set(key,"100");

        executorService = Executors.newScheduledThreadPool(1);
        String renewlua = "" +
                "if redis.call('get',KEYS[1]) == ARGV[1] then redis.call('expire',KEYS[1],ARGV[2]) ; return true " +
                "else return false " +
                "end";
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Iterator<String> iterator = set.iterator();
                while (iterator.hasNext()) {
                    String clientid = iterator.next();
                    Boolean renew = redisTemplate.execute(new RedisCallback<Boolean>() {
                        @Override
                        public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                            Boolean eval = null;
                            try {
                                eval = redisConnection.eval(
                                        renewlua.getBytes(),
                                        ReturnType.BOOLEAN,
                                        1,
                                        lockKey.getBytes(),
                                        clientid.getBytes(),
                                        "5".getBytes()

                                );
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            return eval;
                        }
                    });
                }
            }
        },0,1,TimeUnit.SECONDS);
    }

    @GetMapping("/get/maotai")
    public String seckillMaotai() {

        Integer count = Integer.parseInt(redisTemplate.opsForValue().get(key));
        //如果还有库存
        if (count > 0) {
            //抢到了茅台，库存减一
            redisTemplate.opsForValue().set(key,String.valueOf(count-1));
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
           Integer count = Integer.parseInt(redisTemplate.opsForValue().get(key));
           //如果还有库存
           if (count > 0) {
               //抢到了茅台，库存减一
               redisTemplate.opsForValue().set(key,String.valueOf(count-1));
               //后续操作 do something
               log.info("我抢到茅台了!");
               return "ok";
           }else {
               return "no";
           }
       }
    }

    //锁key
    String lockKey = "maotailock";

    //setnx  setex set
    @GetMapping("/get/maotai3")
    public String seckillMaotai3() {
        //先获取锁,如果能获取到则进行业务操作
//        Boolean execute = redisTemplate.execute(new RedisCallback<Boolean>() {
//            @Override
//            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
//                Boolean aBoolean = redisConnection.setNX(lockKey.getBytes(StandardCharsets.UTF_8), "1".getBytes(StandardCharsets.UTF_8));
//                redisConnection.expire(lockKey.getBytes(StandardCharsets.UTF_8),10);
//                return aBoolean;
//            }
//        });
        String clientId = UUID.randomUUID().toString() + Thread.currentThread().getId();
        Boolean ifAbsent = redisTemplate.opsForValue().setIfAbsent(lockKey, clientId,5,TimeUnit.SECONDS);
        if (ifAbsent) {
            //获取到了锁,给锁设置一个过期时间
            // redisTemplate.expire(lockKey,10,TimeUnit.SECONDS);
            //开始进行业务操作
            try {
                Integer count = Integer.parseInt(redisTemplate.opsForValue().get(key));
                //如果还有库存
                if (count > 0) {
                    //抢到了茅台，库存减一
                    redisTemplate.opsForValue().set(key,String.valueOf(count-1));
                    //后续操作 do something
                    log.info("我抢到茅台了!");
                    //模拟故障退出
                    System.exit(1);
                    return "ok";
                }else {
                    return "no";
                }
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } finally {
                //保证锁一定会被释放
                // 自己的锁自己释放 但是需要保证原子操作，此处无法保证
                String lockvalue = redisTemplate.opsForValue().get(lockKey);
                if (lockvalue !=null && lockvalue.equals(clientId)) {
                    redisTemplate.delete(lockKey);
                }
            }
        }
        return "dont get lock";
    }

    @GetMapping("/get/maotai4")
    public String seckillMaotai4() {
        //先获取锁,如果能获取到则进行业务操作
        String clientId = UUID.randomUUID().toString() + Thread.currentThread().getId();
        /*String locklua = "" +
                "if redis.call('setnx',KEYS[1],ARGV[1]) == 1 then redis.call('expire',KEYS[1],ARGV[2]) ; return true " +
                "else return false " +
                "end";
        Boolean islock = redisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                Boolean lock = redisConnection.eval(
                        locklua.getBytes(),
                        ReturnType.BOOLEAN,
                        1,
                        lockKey.getBytes(),
                        clientId.getBytes(),
                        "5".getBytes()
                );
                return lock;
            }
        });*/
        Boolean islock = redisTemplate.opsForValue().setIfAbsent(lockKey, clientId,5,TimeUnit.SECONDS);//可以改用lua脚本，不改也可以
        if (islock) {
            //开始进行业务操作
            try {
                Integer count = Integer.parseInt(redisTemplate.opsForValue().get(key));
                //如果还有库存
                if (count > 0) {
                    //抢到了茅台，库存减一
                    redisTemplate.opsForValue().set(key,String.valueOf(count-1));
                    //后续操作 do something
                    log.info("我抢到茅台了!");
                    //模拟故障退出
                    // System.exit(1);
                    return "ok";
                }else {
                    return "no";
                }
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } finally {
                //保证锁一定会被释放
                String unlocklua = "" +
                        "if redis.call('get',KEYS[1]) == ARGV[1] then redis.call('del',KEYS[1]); return true " +
                        "else return false " +
                        "end";
                Boolean unlock = redisTemplate.execute(new RedisCallback<Boolean>() {
                    @Override
                    public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                        Boolean unlock = redisConnection.eval(unlocklua.getBytes(),
                                ReturnType.BOOLEAN,
                                1,
                                lockKey.getBytes(),
                                clientId.getBytes());
                        return unlock;
                    }
                });
            }
        }
        return "dont get lock";
    }


    //锁续期（续命）
    @GetMapping("/get/maotai5")
    public String seckillMaotai5() {
        //先获取锁,如果能获取到则进行业务操作
        String clientId = UUID.randomUUID().toString() + Thread.currentThread().getId();
        //Boolean ifAbsent = redisTemplate.opsForValue().setIfAbsent(lockKey, clientId,30,TimeUnit.SECONDS);//可以改用lua脚本，不改也可以
        String locklua = "" +
                "if redis.call('setnx',KEYS[1],ARGV[1]) == 1 then redis.call('expire',KEYS[1],ARGV[2]) ; return true " +
                "else return false " +
                "end"; // SET key uuid NX PX 30000 也可
        Boolean islock = redisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                Boolean lock = redisConnection.eval(
                        locklua.getBytes(),
                        ReturnType.BOOLEAN,
                        1,
                        lockKey.getBytes(),
                        clientId.getBytes(),
                        "5".getBytes()
                );
                return lock;
            }
        });
        if (islock) {
            //加锁成功,将当前获取到锁的clientid提交到队列，由守护线程为其续期
            set.add(clientId);
            //开始进行业务操作
            try {
                Integer count = Integer.parseInt(redisTemplate.opsForValue().get(key));
                //如果还有库存
                if (count > 0) {
                    //抢到了茅台，库存减一
                    redisTemplate.opsForValue().set(key,String.valueOf(count-1));
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
                //锁要释放,也无需在续期了
                set.remove(clientId);
                //保证锁一定会被释放
                String unlocklua = "" +
                        "if redis.call('get',KEYS[1]) == ARGV[1] then redis.call('del',KEYS[1]); return true " +
                        "else return false " +
                        "end";
                Boolean unlock = redisTemplate.execute(new RedisCallback<Boolean>() {
                    @Override
                    public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                        Boolean unlock = redisConnection.eval(unlocklua.getBytes(),
                                ReturnType.BOOLEAN,
                                1,
                                lockKey.getBytes(),
                                clientId.getBytes());
                        return unlock;
                    }
                });
            }
        }
        return "dont get lock";
    }

    @GetMapping("/get/maotai6")
    public String seckillMaotai6() {
        //先加锁
        String clientId = UUID.randomUUID().toString()+Thread.currentThread().getId();
        lock(lockKey,clientId,"5");
        try {
            Integer count = Integer.parseInt(redisTemplate.opsForValue().get(key));
            //如果还有库存
            if (count > 0) {
                //抢到了茅台，库存减一
                redisTemplate.opsForValue().set(key,String.valueOf(count-1));
                //后续操作 do something
                log.info("我抢到茅台了!");
                return "ok";
            }else {
                return "no";
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } finally {
            //解锁
            unlock(lockKey,clientId);
        }
        return "dont get lock";
    }


    //支持锁重入
    private static ThreadLocal<Map<String, Integer>> LOCKERS = ThreadLocal.withInitial(HashMap::new);
    // 加锁
    public boolean lock(String key,String value,String expiration) {
        Map<String, Integer> lockers = LOCKERS.get();
        if (lockers.containsKey(key)) {
            lockers.put(key, lockers.get(key) + 1);
            return true;
        } else {
            if (tryLock(key,value,expiration)) {
                lockers.put(key, 1);
                return true;
            }
        }
        return false;
    }
    public boolean tryLock(String lockName,String clientId,String expiration) {
        String locklua = "" +
                "if redis.call('setnx',KEYS[1],ARGV[1]) == 1 then redis.call('expire',KEYS[1],ARGV[2]) ; return true " +
                "else return false " +
                "end";
        Boolean islock = redisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                Boolean lock = redisConnection.eval(
                        locklua.getBytes(),
                        ReturnType.BOOLEAN,
                        1,
                        lockName.getBytes(),
                        clientId.getBytes(),
                        expiration.getBytes()
                );
                return lock;
            }
        });
        return  islock;
    }

    // 解锁
    public void unlock(String key,String clientid) {
        Map<String, Integer> lockers = LOCKERS.get();
        if (lockers.getOrDefault(key, 0) <= 1) {
            lockers.remove(key);
            //DEL key
            tryUnlock(key,clientid);
        } else {
            lockers.put(key, lockers.get(key) - 1);
        }
    }

    public boolean tryUnlock(String lockName,String clientid) {
        String unlocklua = "" +
                "if redis.call('get',KEYS[1]) == ARGV[1] then redis.call('del',KEYS[1]); return true " +
                "else return false " +
                "end";
        Boolean unlock = redisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                Boolean unlock = redisConnection.eval(unlocklua.getBytes(),
                        ReturnType.BOOLEAN,
                        1,
                        lockName.getBytes(),
                        clientid.getBytes());
                return unlock;
            }
        });
        return  unlock;
    }

    /**
     * 本地记录重入次数虽然高效,但如果考虑过期时间在本地和redis的一致性问题，就会增加代码的复杂度。故另外一种支持重入特性的是使用redis的hash类型
     * 既存锁的标识也对重入次数进行计数
     * // 如果 lock_key 不存在
     * if (redis.call('exists', KEYS[1]) == 0)
     * then
     *     // 设置 lock_key 线程标识 1 进行加锁
     *     redis.call('hset', KEYS[1], ARGV[2], 1);
     *     // 设置过期时间
     *     redis.call('pexpire', KEYS[1], ARGV[1]);
     *     return nil;
     *     end;
     * // 如果 lock_key 存在且线程标识是当前欲加锁的线程标识
     * if (redis.call('hexists', KEYS[1], ARGV[2]) == 1)
     *     // 自增
     *     then redis.call('hincrby', KEYS[1], ARGV[2], 1);
     *     // 重置过期时间
     *     redis.call('pexpire', KEYS[1], ARGV[1]);
     *     return nil;
     *     end;
     * // 如果加锁失败，返回锁剩余时间
     * return redis.call('pttl', KEYS[1]);
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

    @GetMapping("/get/maotai7")
    public String seckillMaotai7() {

        RLock lock = redissonClient.getLock(lockKey);
        //加锁
        lock.lock();
        try {
            Integer count = Integer.parseInt(redisTemplate.opsForValue().get(key));
            //如果还有库存
            if (count > 0) {
                //抢到了茅台，库存减一
                redisTemplate.opsForValue().set(key,String.valueOf(count-1));
                //后续操作 do something
                log.info("我抢到茅台了!");
                return "ok";
            }else {
                return "no";
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } finally {
            //释放锁
            lock.unlock();
        }
        return "dont get lock";
    }


    public String redlock() {
        Config config1 = new Config();
        config1.useSingleServer().setAddress("redis://192.168.200.129:6379");
        RedissonClient redissonClient1 = Redisson.create(config1);

        Config config2 = new Config();
        config2.useSingleServer().setAddress("redis://192.168.200.129:6380");
        RedissonClient redissonClient2 = Redisson.create(config2);

        Config config3 = new Config();
        config3.useSingleServer().setAddress("redis://192.168.200.129:6381");
        RedissonClient redissonClient3 = Redisson.create(config3);

        String resourceName = "REDLOCK";

        RLock lock1 = redissonClient1.getLock(resourceName);
        RLock lock2 = redissonClient2.getLock(resourceName);
        RLock lock3 = redissonClient3.getLock(resourceName);
        RedissonRedLock redLock = new RedissonRedLock(lock1, lock2, lock3);
        boolean isLock;
        try {
            // isLock = redLock.tryLock();
            // 500ms拿不到锁, 就认为获取锁失败。10000ms即10s是锁失效时间。
            isLock = redLock.tryLock(500, 10000, TimeUnit.MILLISECONDS);
            System.out.println("isLock = "+isLock);
            if (isLock) {
                //TODO 这里写业务逻辑
            }
        } catch (Exception e) {
        } finally {
            // 无论如何, 最后都要解锁
            redLock.unlock();
        }
        return "ok";
    }






}
