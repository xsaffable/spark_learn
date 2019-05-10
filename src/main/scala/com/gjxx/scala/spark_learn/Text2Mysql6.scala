package com.gjxx.scala.spark_learn

import java.util.{Properties, UUID}

import com.gjxx.scala.spark_learn.Text2Mysql5.sc
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{RowFactory, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object Text2Mysql6 {

  val sc = new SparkContext("local[4]", "TextToMysql")
  val spark = SparkSession.builder().getOrCreate()

  val prop = new Properties()
  prop.put("user", "root")
  prop.put("password", "1234")
  prop.put("url", "jdbc:mysql://47.93.4.111:3306/library?useUnicode=true&characterEncoding=utf-8")
  prop.put("tableName", "book")

  def main(args: Array[String]): Unit = {

    val fileRDD = sc.textFile("C:/Users/Admin/Desktop/书籍采集/2019-4-30-现代文学.txt")
    val rdd = fileRDD.map(line => {
      if (line.contains(":")) {
        line.split(":")(1).trim
      } else {
        line.trim
      }
    }).filter(_.contains("/"))
      .map(_.split("/"))
      .filter(_.length >= 2)
      .filter(_(0).contains(","))
      .map(line => {
        val fields = line(0).split(",")
        (fields, line(1))
      }).filter(_._1.length == 2)
      .map(line => {
        (line._1(0), line._1(1), line._2)
      }).filter(line => {
      if (line._3.contains("Press") || line._3.contains("press") || line._3.contains("出版社")) {
        true
      } else {
        false
      }
    }).map(line => {
      RowFactory.create(UUID.randomUUID().toString, line._1, line._2, line._3, "现代文学类")
    })


    val structFields = new ListBuffer[StructField]
    structFields.append(DataTypes.createStructField("id", DataTypes.StringType, false))
    structFields.append(DataTypes.createStructField("book_name", DataTypes.StringType, false))
    structFields.append(DataTypes.createStructField("author", DataTypes.StringType, false))
    structFields.append(DataTypes.createStructField("press", DataTypes.StringType, false))
    structFields.append(DataTypes.createStructField("category", DataTypes.StringType, false))
    // 构建StructType，用于最后的DataFrame元数据描述
    val structType = DataTypes.createStructType(structFields.toArray)
    // 构造DataFrame
    val df = spark.createDataFrame(rdd, structType)

    df.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), prop.getProperty("tableName"), prop)

    sc.stop()

  }

}
