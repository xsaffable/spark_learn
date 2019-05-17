package com.gjxx.scala.log_analysis

import java.util.Properties

import com.gjxx.scala.log_analysis.DataDeal.BorrowRecord
import org.apache.spark.sql.{DataFrame, SparkSession}

object BooksData {

  val prop = new Properties()
  prop.put("user", "root")
  prop.put("password", "1234")
  prop.put("url", "jdbc:mysql://47.93.4.111:3306/library?useUnicode=true&characterEncoding=utf-8")
  prop.put("tableName", "book")

  /**
    * 获得所有的书
    * @param spark
    * @param prop
    * @return
    */
  def getAllBooks(spark: SparkSession): DataFrame = {
    spark.read.jdbc(prop.getProperty("url"), prop.getProperty("tableName"), prop)
  }

  /**
    * 制作数据集
    * 包含所有的书
    * 1->借阅
    * 0->未借阅
    * @return
    */
  def makeDataSet(brs: List[BorrowRecord], books: DataFrame): List[Book] = {
    val booksRDDRow = books.rdd
    val booksArray = booksRDDRow.map(row => {
      Book(row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString, "0")
    }).collect()
    for (i <- brs.indices) {
      booksArray.foreach(book => {
        if (book.book_id == brs(i).bookId) {
          book.borred = "1"
        }

      })
    }
    booksArray.toList
  }

  case class Book(book_id_str: String, book_name_str: String, author_str: String, press_str: String, category_str: String, borred_str: String) {
    var book_id = book_id_str
    var book_name = book_name_str
    var author = author_str
    var press = press_str
    var category = category_str
    var borred = borred_str
  }

}
