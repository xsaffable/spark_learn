package com.gjxx.java.java.learn.concurrent.chapter06;

import java.util.concurrent.locks.LockSupport;

/**
 * @author Sxs
 * @description LockSupport
 * @date 2019/10/31 18:22
 */
public class Demo {

    public static void main(String[] args) {
        System.out.println("begin park!");
        LockSupport.unpark(Thread.currentThread());
        LockSupport.park();
        System.out.println("end park!");
    }

}
