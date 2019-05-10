package com.gjxx.scala.spark_learn

import java.util.{Properties, UUID}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RowFactory, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField}

import scala.collection.mutable.ListBuffer

class Text2Mysql {

  val sc = new SparkContext("local[4]", "TextToMysql")
  val spark = SparkSession.builder().getOrCreate()

  val prop = new Properties()
  prop.put("user", "root")
  prop.put("password", "1234")
  prop.put("url", "jdbc:mysql://47.93.4.111:3306/library?useUnicode=true&characterEncoding=utf-8")
  prop.put("tableName", "book")

  /**
    * 读取文件
    * @param path
    * @return
    */
  def readTextFile(path: String): RDD[(List[String], String)] = {
    val fileRDD = sc.textFile(path, 4)
    val fRDD = fileRDD.map(_.split(":"))
      .filter(_.length == 2)
      .map(_(1).split("/"))
      .filter(_.length == 4)
      .filter(_(0).contains(","))
      .map(line => (line(0).split(",").toList, line(1)))
    fRDD
  }

  /**
    * 转换成rows
    * @param rdd
    * @return
    */
  def tranRows(rdd: RDD[(List[String], String)]): DataFrame = {
    val rdd2 = rdd.map(line => {
      RowFactory.create(UUID.randomUUID().toString, line._1(0), line._1(1), line._2, "现代科幻类")
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
    val stuDF = spark.createDataFrame(rdd2, structType)
    stuDF
  }

  /**
    * 写入到mysql
    */
  def toMySql(df: DataFrame): Unit = {
//    df.write.mode(SaveMode.Append).format("org.apache.spark.sql.execution.datasources.jdbc.DefaultSource")
//      .options(Map(
//        "url" -> prop.getProperty("url"),
//        "driver" -> prop.getProperty("driver"),
//        "dbtable" -> prop.getProperty("tableName"),
//        "user" -> prop.getProperty("user"),
//        "password" -> prop.getProperty("password"))
//      ).save

    df.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), prop.getProperty("tableName"), prop)
  }

  def close(): Unit = {
    sc.stop()
  }

  def start(): Unit = {
    val fileRDD1 = readTextFile("C:/Users/Admin/Desktop/书籍采集/2019-4-29-现代科幻.txt")
    val file1DF = tranRows(fileRDD1)
    toMySql(file1DF)
    close()
  }

}

object Text2Mysql {
  def main(args: Array[String]): Unit = {
    new Text2Mysql().start()
  }
}
