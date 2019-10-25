package com.gjxx.java.java.learn.concurrent.chapter01;

/**
 * @author Sxs
 * @description ThreadLocal 使用示例
 * @date 2019/10/21 10:22
 */
public class ThreadTest3 {

    private static ThreadLocal<String> localVariable = new ThreadLocal<>();

    public static void main(String[] args) {
        Thread threadOne = new Thread(() -> {
            // 设置本地变量值
            localVariable.set("threadOne local variable");
            print("threadOne");
            System.out.println("threadOne remove after:" + localVariable.get());
        });
        Thread threadTwo = new Thread(() -> {
            // 设置本地变量值
            localVariable.set("threadTwo local variable");
            print("threadTwo");
            System.out.println("threadTwo remove after:" + localVariable.get());
        });

        // 启动线程
        threadOne.start();
        threadTwo.start();

    }

    private static void print(String str) {
        // 打印当前线程本地内存中的localVariable变量的值
        System.out.println(str + ":" + localVariable.get());
        // 清除当前线程本地内存中的localVariable变量
        localVariable.remove();
    }

}
