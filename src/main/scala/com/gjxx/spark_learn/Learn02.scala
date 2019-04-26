package com.gjxx.spark_learn

import org.apache.spark.SparkContext

/**
  * 注释
  */
class Learn02 {
  def start(): Unit = {
    val sc = new SparkContext("local[4]", "Learn02")
    val pre_data = sc.textFile("hdfs://cdh1/test/f20674.dat")
    val fields = pre_data.map(line => line.trim().replace("  ", " ").replace(" ", ""))
      .foreach(println(_))

  }
}

object Learn02 {

  def main(args: Array[String]): Unit = {
    new Learn02().start()
  }

}
