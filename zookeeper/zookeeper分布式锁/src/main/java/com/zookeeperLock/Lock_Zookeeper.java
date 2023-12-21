package com.zookeeperLock;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;

@RestController
public class Lock_Zookeeper {

    private  final static Logger log = LoggerFactory.getLogger(Lock_Zookeeper.class);


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
     */


    private final static String PATH = "/rootlock02";

    @Autowired
    ZkConfiguration zkConfiguration;

    String lockey = "maotailock";

    @GetMapping("/get/maotai3")
    public String seckillMaotai3() throws Exception {

        //获取锁
        InterProcessMutex lock = new InterProcessMutex(zkConfiguration.curatorFramework(), PATH);
        lock.acquire();
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
        }finally {
            //释放锁
            lock.release();
        }
        return "dont get lock";



    }




   /* @GetMapping("/get/maotai6")
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
    }*/



}
