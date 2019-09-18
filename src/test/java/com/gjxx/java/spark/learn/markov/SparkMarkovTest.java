package com.gjxx.java.spark.learn.markov;

import org.apache.commons.lang.time.DateUtils;

import java.text.ParseException;

import static org.junit.Assert.*;

/**
 * @Description TODO
 * @Author Sxs
 * @Date 2019/9/18 20:31
 * @Version 1.0
 */
public class SparkMarkovTest {

    public static void main(String[] args) throws ParseException {
        System.out.println(DateUtils.parseDate("2013-01-01", new String[]{"yyyy-MM-dd"}));
    }

}