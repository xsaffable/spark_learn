package com.gjxx.spark_learn

import org.apache.spark.SparkContext

class Learn01 {

}

object Learn01 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[4]", "SparkLearn01")

    val rdd = sc.textFile("hdfs://cdh1/test/test.txt")

    val rdd2 = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    rdd2.foreach(println(_))

    sc.stop()
  }
}
