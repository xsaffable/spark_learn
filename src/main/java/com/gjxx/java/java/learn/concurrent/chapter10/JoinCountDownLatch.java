package com.gjxx.java.java.learn.concurrent.chapter10;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * @author Sxs
 * @description countDownLatch 应用
 * @date 2020/1/15 19:06
 */
public class JoinCountDownLatch {

    /**
     * 创建一个 CountDownLatch 实例
     */
    private static CountDownLatch countDownLatch = new CountDownLatch(3);

    public static void main(String[] args) throws InterruptedException {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("demo-pool-%d").build();
        ExecutorService executorService = new ThreadPoolExecutor(2, 2,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<Runnable>(),
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy());

        // 添加线程到线程池
        executorService.submit(() -> {
            try {
                Thread.sleep(1000);
                System.out.println("child threadOne over!");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
        });

        executorService.submit(() -> {
            try {
                Thread.sleep(1000);
                System.out.println("child threadTwo over!");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
        });

        executorService.submit(() -> {
            try {
                Thread.sleep(1000);
                System.out.println("child threadThree over!");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
        });

        System.out.println("wait all child thread over!");

        // 等待子线程执行完毕，返回
        countDownLatch.await();

        System.out.println("all child thread over!");

        executorService.shutdown();

    }

}
