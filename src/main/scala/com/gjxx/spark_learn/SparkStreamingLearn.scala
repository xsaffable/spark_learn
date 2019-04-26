package com.gjxx.spark_learn

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

class SparkStreamingLearn {

  def start(): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("SparkStreamingLearn")
    // 批处理间隔为1s
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }

}

object SparkStreamingLearn {
  def main(args: Array[String]): Unit = {
    new SparkStreamingLearn().start()
  }
}
