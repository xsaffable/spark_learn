package com.gjxx.java.phoenix;

import lombok.extern.log4j.Log4j2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author sxs
 */
@Log4j2
public class Demo {

    public static void main(String[] args) {
        // 创建sparkSession
        SparkSession sparkSession = SparkSession.builder()
                .appName("Demo")
                .master("local[12]")
                .getOrCreate();

        // 读取表数据
        Dataset<Row> ds = sparkSession.read().format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("url", "jdbc:phoenix:cdh1:2181")
                .option("dbtable", "\"student\"")
                .load();

        ds.show();

        sparkSession.close();
    }

}
