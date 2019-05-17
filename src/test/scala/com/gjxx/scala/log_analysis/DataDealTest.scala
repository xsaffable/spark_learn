package com.gjxx.scala.log_analysis

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DataDealTest {

  val sc = new SparkContext("local[4]", "TextToMysql")
  val spark = SparkSession.builder().getOrCreate()

  def main(args: Array[String]): Unit = {
    DataDeal.readFile("E:/borrowInfo.log", sc)
        .foreach(println(_))


    sc.stop()
  }

}
