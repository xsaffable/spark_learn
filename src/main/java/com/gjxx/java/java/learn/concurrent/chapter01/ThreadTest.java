package com.gjxx.java.java.learn.concurrent.chapter01;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * @author Sxs
 * @description 线程创建与运行
 * @date 2019/10/14 17:11
 */
public class ThreadTest {

    public static class MyThread extends Thread {
        @Override
        public void run() {
            System.out.println("I am a child thread");
        }
    }

    public static class RunnableTask implements Runnable {
        private String test;

        public RunnableTask(String test) {
            this.test = test;
        }

        @Override
        public void run() {
            System.out.println("I am a child thread" + test);
        }
    }

    public static class CallerTask implements Callable<String> {
        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        @Override
        public String call() throws Exception {
            return "hello";
        }
    }

    public static void main(String[] args) {
        // 创建线程
        Thread thread = new MyThread();
        // 启动线程
        thread.start();

        String test = " test";

        RunnableTask task = new RunnableTask(test);
        new Thread(task).start();
        new Thread(task).start();

        // 创建异步任务
        FutureTask<String> futureTask = new FutureTask<>(new CallerTask());
        // 启动线程
        new Thread(futureTask).start();
        try {
            // 等待任务执行完毕，并返回结果
            String result = futureTask.get();
            System.out.println(result);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

}
