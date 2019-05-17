package com.gjxx.scala.log_analysis

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object DataDeal {

  /**
    * 读取log
    * 并预处理数据
    * 根据userId分组
    * @param path
    * @param sc
    * @return
    */
  def readFile(path: String, sc: SparkContext): RDD[(String, List[BorrowRecord])] = {
    val fileRDD = sc.textFile(path, 4)

    // 去除干扰字段
    fileRDD.map(_.split("\\{borrowInfo\\:")(1).trim)
      .map(_.split("\\]\\,"))
      // 转换成BorrowRecord对象
      .map(toBorrowRecord)
      // 去除还书的记录
      .filter(x => x.asInstanceOf[BorrowRecord].isBorrow == "0")
      .map(getUserId)
      .groupByKey() // 按userId分组，获得每个用户所有的借阅记录
      .map(line => (line._1, line._2.toList)) // 把借阅记录转换成list

  }

  /**
    * 把userId提取出来作为key，用于分组
    */
  val getUserId = (o: Object) => {
    val br = o.asInstanceOf[BorrowRecord]
    (br.userId, br)
  }

  /**
    * 函数:
    * 把数据转换成对象
    */
  val toBorrowRecord = (arr: Array[String]) => {
    try {
      val userId = arr(0).split("=")(1)
      val username = arr(1).split("=")(1)
      val bookId = arr(2).split("=")(1)
      val bookName = arr(3).split("=")(1)
      val author = arr(4).split("=")(1)
      val press = arr(5).split("=")(1)
      val category = arr(6).split("=")(1)
      val isBorrow = arr(7).split("=")(1)
      val timeStr = arr(8).split("=")(1)
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val time = sdf.parse(timeStr)
      BorrowRecord(userId, username, bookId, bookName, author, press, category, isBorrow, time)
    } catch { // 出现ArrayIndexOutOfBoundsException,即数据有缺失,去除本条数据
      case ex: ArrayIndexOutOfBoundsException => {
        println("start error------------------------------")
        println(ex)
        println("error: " + arr.toList)
        println("end error------------------------------")
        BorrowRecord
      }
    }
  }

  /**
    * 静态类:
    * 借阅记录对象,用来存放一条借阅信息
    * @param userId 用户id
    * @param username 用户名
    * @param bookId 书籍id
    * @param bookName 书名
    * @param author 作者
    * @param press 出版社
    * @param category 分类
    * @param isBorrow 借阅->0,归还->1
    * @param time
    */
  case class BorrowRecord(userId: String, username: String, bookId: String, bookName: String,
                          author: String, press: String, category: String, isBorrow: String, time: Date)


}
