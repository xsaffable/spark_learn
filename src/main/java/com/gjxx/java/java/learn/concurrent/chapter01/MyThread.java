package com.gjxx.java.java.learn.concurrent.chapter01;

public class MyThread {

     boolean flag = true;

     void startWhile(){
        while (flag) {
            System.out.println("flag = true");
        }
    }


    public static void main(String[] args) {
        MyThread myThread = new MyThread();
        new Thread(() -> myThread.startWhile()).start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        myThread.flag = false;
        System.out.println("exit");
    }
}