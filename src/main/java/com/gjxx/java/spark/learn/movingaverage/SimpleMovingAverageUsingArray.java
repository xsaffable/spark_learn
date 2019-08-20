package com.gjxx.java.spark.learn.movingaverage;

/**
 * @Description 使用数组实现移动平均
 * @Author Sxs
 * @Date 2019/8/20 15:10
 * @Version 1.0
 */
public class SimpleMovingAverageUsingArray {

    private double sum = 0.0;
    private final int period;
    private double[] window = null;
    private int pointer = 0;
    private int size = 0;

    public SimpleMovingAverageUsingArray(int period) {
        if (period < 1) {
            throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
        window = new double[period];
    }

    /**
     * @Author Sxs
     * @Description 求和; 方法参数: 下一个元素的值
     * @Param [number]
     * @return void
     * @Date 18:04 2019/8/20
     */
    public void addNewNumber(double number) {
        sum += number;
        if (size < period) {
            window[pointer++] = number;
            size++;
        } else {
            // 给数组的前period个位置，循环赋值
            pointer = pointer % period;
            sum -= window[pointer];
            window[pointer++] = number;
        }
    }

    /**
     * @Author Sxs
     * @Description 计算平均值; 方法参数:
     * @Param []
     * @return double
     * @Date 18:07 2019/8/20
     */
    public double getMovingAverage() {
        if (size == 0) {
            throw new IllegalArgumentException("average is undefined");
        }
        return sum / size;
    }

}
