package com.gjxx.java.phoenix.learn;

import lombok.extern.log4j.Log4j2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Properties;

/**
 *  phoenix的工具类
 * @author sxs
 */
@Log4j2
public class PhoenixUtil {

    /**
     * 存储phoenix相关配置的properties
     */
    private static Properties prop = new Properties();

    /**
     * 创建sparkSession
     */
    private static SparkSession sparkSession = SparkSession.builder()
            .appName("PhoenixUtil")
            .master("local[12]")
            .getOrCreate();

    /**
     * 读取数据
     * @param dbtable 要读取的表名
     */
    public static void read(String dbtable) {

        try {
            prop.load(ClassLoader.getSystemResourceAsStream("phoenix.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        log.info("start read ...");

        // 读取表数据
        Dataset<Row> ds = sparkSession.read().format("jdbc")
                .option("driver", prop.getProperty("phoenix.driver"))
                .option("url", prop.getProperty("phoenix.url"))
                .option("dbtable", dbtable)
                .load();

        ds.show();

        log.info("end read ...");
    }

    /**
     * 关闭Spark Session
     */
    public static void close() {
        sparkSession.close();
    }

}
