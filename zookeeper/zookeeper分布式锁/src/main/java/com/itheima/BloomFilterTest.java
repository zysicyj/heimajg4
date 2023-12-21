package com.itheima;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.junit.Test;

public class BloomFilterTest {

    private static int capacity = 100000;

    private static double fpp = 0.01;

    @Test
    public  void test1() {
        BloomFilter<Integer> bloomFilter = BloomFilter.create(Funnels.integerFunnel(), capacity, fpp);
        // 插入10万样本数据
        for (int i = 0; i < capacity; i++) {
            bloomFilter.put(i);
        }

        // 用另外十万测试数据，测试误判率
        int count = 0;
        for (int i = capacity; i < capacity + 100000; i++) {
            if (bloomFilter.mightContain(i)) {
                count++;
                System.out.println(i + "误判了");
            }
        }
        System.out.println("总共的误判数:" + count);
    }
}

