package com.gjxx.java.spark.learn.conf;

/**
 * @ClassName SparkConf
 * @Description spark相关的配置
 * @Author SXS
 * @Date 2019/7/19 9:29
 * @Version 1.0
 */
public interface SparkConf {

    /**
     * 测试使用的master
     */
    String MASTER = "local[8]";

//    /**
//     * 正式环境使用的master
//     */
//    String MASTER = null;

    /**
     * @Author SXS
     * @Description app name
     * @Date 9:42 2019/7/19
     * @Param
     * @return
     */
    String APP_NAME = "sparkProgram";

}
