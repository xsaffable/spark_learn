package com.gjxx.java.java.learn.concurrent.chapter01;

import java.util.concurrent.TimeUnit;

public class VolatileFaceThread {
    boolean isRunning = true;

    void m() {
        System.out.println("isRunning start");
        while (isRunning) {
          System.out.println("isRunning while true");
        }
        System.out.println("isRunning end");
    }

    public static void main(String[] args) {
        VolatileFaceThread vft = new VolatileFaceThread();
        new Thread(vft::m).start();
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        vft.isRunning = false;
        System.out.println("update isRunning...");
    }
}