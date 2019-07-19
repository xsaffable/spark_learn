package com.gjxx.java.spark.learn.utils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author Admin
 */
public class SparkUtil {

    /**
     * @Author SXS
     * @Description 获得SparkSession
     * @Date 9:27 2019/7/19
     * @Param [appName, master]
     * @return org.apache.spark.sql.SparkSession
     */
    public static SparkSession getSparkSession(String appName, String master) {
        if (master != null) {
            return SparkSession.builder()
                    .appName(appName)
                    .master(master)
                    .getOrCreate();
        } else {
            return SparkSession.builder()
                    .appName(appName)
                    .getOrCreate();
        }

    }

    /**
     * 获得JavaSparkContext
     * @param appName String
     * @param master String
     * @return JavaSparkContext
     */
    public static JavaSparkContext getJSC(String appName, String master) {
        SparkSession sparkSession = getSparkSession(appName, master);
        return new JavaSparkContext(sparkSession.sparkContext());
    }

}
