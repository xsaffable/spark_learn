package com.gjxx.java.spark.learn.friendrecommendation;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @Description TODO
 * @Author Sxs
 * @Date 2019/9/16 8:58
 * @Version 1.0
 */
public class SparkFriendRecommendationTest {

    @Test
    public void test() {
        Map<Long, Long> map = new HashMap<>();
        map.put(1L, 10L);
        map.put(2L, 12L);
        map.put(1L, 2L);
        map.forEach((k, v) -> System.out.println(k + "->" + v));
    }

}