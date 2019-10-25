package com.gjxx.java.java.learn.concurrent.chapter03;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Sxs
 * @description ThreadLocalRandom测试
 * @date 2019/10/23 18:10
 */
public class ThreadLocalRandomTest {

    public static void main(String[] args) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        for (int i = 0; i < 10; i++) {
            System.out.println(random.nextInt(5));
        }
    }

}
