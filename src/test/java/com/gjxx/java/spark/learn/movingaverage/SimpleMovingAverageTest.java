package com.gjxx.java.spark.learn.movingaverage;

import static org.junit.Assert.*;

/**
 * @Description TODO
 * @Author Sxs
 * @Date 2019/8/20 18:07
 * @Version 1.0
 */
public class SimpleMovingAverageTest {

    public static void main(String[] args) {
        double[] testData = {10, 18, 20, 30, 24, 33, 27};
        int[] allWindowSizes = {3, 4};
        for (int allWindowSize : allWindowSizes) {
            SimpleMovingAverage sma = new SimpleMovingAverage(allWindowSize);
            for (double x : testData) {
                sma.addNewNumber(x);
                System.out.println(sma.getMovingAverage());
            }
            System.out.println("-----------");
        }
    }

}