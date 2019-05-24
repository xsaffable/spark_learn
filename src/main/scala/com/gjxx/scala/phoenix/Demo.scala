package com.gjxx.scala.phoenix

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

import org.apache.phoenix.spark._

object Demo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[12]").appName("phoenix").getOrCreate()
    val conf = new Configuration()
    conf.set("hbase.zookeeper.quorum", "cdh1:2181,cdh2:2181,cdh3:2181")

    //读取
    val df = spark.sqlContext.phoenixTableAsDataFrame("student", Array("ROW", "district"), conf = conf)

//    val df = spark.read
//        .format("org.apache.phoenix.spark")
//        .option("table", "student")
//        .option("zkUrl", "cdh1:2181")
//        .load()

    df.show()

  }

}
