package com.gjxx.scala.spark_learn

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Text2Mysql2 {

  val sc = new SparkContext("local[4]", "TextToMysql")
  val spark = SparkSession.builder().getOrCreate()

  val prop = new Properties()
  prop.put("user", "root")
  prop.put("password", "1234")
  prop.put("url", "jdbc:mysql://47.93.4.111:3306/library?useUnicode=true&characterEncoding=utf-8")
  prop.put("tableName", "book")

  def main(args: Array[String]): Unit = {
    val fileRDD = sc.textFile("C:/Users/Admin/Desktop/书籍采集/2019-4-30-编程.txt")
    fileRDD.map(_.split(":"))
      .filter(_.length == 2)
      .map(_(1).split("/"))
      .filter(_.length >= 3)
//      .map(line => {
//        (line(0).split(",")(0), line(1))
//      })
        .foreach(line => println(line.toList))

    sc.stop()
  }

}
