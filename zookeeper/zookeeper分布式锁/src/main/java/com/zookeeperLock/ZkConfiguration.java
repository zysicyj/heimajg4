package com.zookeeperLock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZkConfiguration {

    @Autowired
    WrapperZk wrapperZk;

    @Bean(initMethod = "start")
    public CuratorFramework curatorFramework(){
        RetryPolicy retrYPolicy = new ExponentialBackoffRetry(wrapperZk.getBaseSleepTimeMs(), wrapperZk.getRetryCount());
        CuratorFramework client = CuratorFrameworkFactory.newClient(wrapperZk.getConnectString(),retrYPolicy);
        return client;
    }


}
