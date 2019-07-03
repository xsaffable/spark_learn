package com.gjxx.scala.saprk_mllib

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

class Demo01 {

  def process(): Unit = {
    val conf = new SparkConf().setAppName("Demo01").setMaster("local")
    val sc = new SparkContext(conf)

    // 读取垃圾邮件
    val spam = sc.textFile("file/spam.txt")
    // 读取普通邮件
    val ham = sc.textFile("file/ham.txt")

    // 创建一个HashingTF实例来吧邮件文件映射为包含10000个特征的向量
    val tf = new HashingTF(10000)
    // 各邮件都被切分为单词，每个单词被映射为一个特征
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val hamFeatures = ham.map(email => tf.transform(email.split(" ")))

    // 创建LabeledPoint数据集分别存放阴性(垃圾邮件)和阳性(正常邮件)的例子
    val negativeExamples = spamFeatures.map(features => LabeledPoint(0, features))
    val positiveExamples = hamFeatures.map(features => LabeledPoint(1, features))
    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache()

    // 使用SGD算法运行逻辑回归
    val model = new LogisticRegressionWithSGD().run(trainingData)

    val postTest = tf.transform("Hi Dad, I started studying Spark ...".split(" "))
    val negTest = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))

    println("Prediction for positive example: " + model.predict(postTest))
    println("Prediction for negative example: " + model.predict(negTest))

  }

}

object Demo01 {

  def main(args: Array[String]): Unit = {
    new Demo01().process()
  }

}
