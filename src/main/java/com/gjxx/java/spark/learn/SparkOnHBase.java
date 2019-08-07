package com.gjxx.java.spark.learn;

import com.gjxx.java.spark.learn.conf.SparkConf;
import com.gjxx.java.spark.learn.utils.HBaseUtil;
import com.gjxx.java.spark.learn.utils.SparkUtil;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.sql.SparkSession;

/**
 * @ClassName SparkOnHBase
 * @Description TODO
 * @Author SXS
 * @Date 2019/7/19 9:34
 * @Version 1.0
 */
public class SparkOnHBase {

    /**
     * 获取SparkSession
     */
    private SparkSession sparkSession = SparkUtil.getSparkSession(SparkConf.APP_NAME, SparkConf.MASTER);

    /**
     * 获取JavaHBaseContext
     * 用于读取HBase表
     */
    JavaHBaseContext jhc = HBaseUtil.getJHC(sparkSession);

    private void test() {

    }

}
