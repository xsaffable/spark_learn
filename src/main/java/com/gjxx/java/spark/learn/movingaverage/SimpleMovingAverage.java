package com.gjxx.java.spark.learn.movingaverage;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @Description 使用队列实现移动平均
 * @Author Sxs
 * @Date 2019/8/20 15:04
 * @Version 1.0
 */
public class SimpleMovingAverage {

    private double sum = 0.0;
    private final int period;
    private final Queue<Double> window = new LinkedList<>();

    /**
     * @Author Sxs
     * @Description 构造器 方法参数: 要计算的时期
     * @Param [period]
     * @Date 15:21 2019/8/20
     */
    public SimpleMovingAverage(int period) {
        if (period < 1) {
            throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
    }

    /**
     * @Author Sxs
     * @Description 求和; 方法参数: 下一个元素的值
     * @Param [number]
     * @return void
     * @Date 15:28 2019/8/20
     */
    public void addNewNumber(double number) {
        sum += number;
        window.add(number);
        if (window.size() > period) {
            // 删除队列头部的元素
            sum -= window.remove();
        }
    }

    /**
     * @Author Sxs
     * @Description 计算平均值; 方法参数:
     * @Param []
     * @return double
     * @Date 15:33 2019/8/20
     */
    public double getMovingAverage() {
        if (window.isEmpty()) {
            throw new IllegalArgumentException("average is undefined");
        }
        return sum / window.size();
    }

}
