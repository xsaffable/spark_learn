package com.gjxx.java.java.learn.concurrent.chapter01;

import sun.misc.Unsafe;

/**
 * @author Sxs
 * @description Unsafe测试使用
 * @date 2019/10/21 18:37
 */
public class ThreadTest4 {

    /**
     * 获取Unsafe的实例
     */
    static final Unsafe unsafe = Unsafe.getUnsafe();

    /**
     * 记录变量state在类TestUnSafe中的偏移值
     */
    static final long stateOffset;

    static {
        try {
            // 获取state变量在类中的偏移值
            stateOffset = unsafe.objectFieldOffset(ThreadTest4.class.getDeclaredField("state"));
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            throw new Error(e);
        }
    }

    public static void main(String[] args) {
        // 创建实例，并设置state的值为1
        ThreadTest4 t = new ThreadTest4();
        boolean success = unsafe.compareAndSwapInt(t, stateOffset, 0, 1);
        System.out.println(success);
    }

}
