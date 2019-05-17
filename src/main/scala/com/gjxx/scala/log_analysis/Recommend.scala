package com.gjxx.scala.log_analysis

import com.gjxx.scala.log_analysis.BooksData.Book
import com.gjxx.scala.log_analysis.DataDeal.BorrowRecord
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Recommend {

  val sc = new SparkContext("local[12]", "Recommend")
  val spark = SparkSession.builder().getOrCreate()

  def start(k: Int): Unit = {

    // 获得所有的记录，并根据userId对记录进行分组
    // 每一条对应着每一个user的所有借阅记录
    val r = DataDeal.readFile("E:/borrowInfo.log", sc).collect()

    val allBooksDF = BooksData.getAllBooks(spark)

    for (i <- r.indices) {
      val userId = r(i)._1
      val borrBooksList = r(i)._2

      // 获得被标记的书籍
      val dataSet = BooksData.makeDataSet(borrBooksList, allBooksDF)
      val dataSetList = tranList(dataSet)
      // 获得没有被借阅过的所有书籍
      val noBorrBooksRDD = dataSet.filter(_.borred == "0")

      // 获得每本没有借过书的预测类型
      val preBorrBooks = noBorrBooksRDD.map(book => {
        // knn计算该书的分类
        // 0->不该被借
        // 1->应该被借
        val bookType = Knn.run(dataSetList, BookModel(book, null), k)
        book.borred = bookType
        book
      })

      // 取到预测的用户喜欢的书籍对应的book_id
      val recomm_record = preBorrBooks.filter(_.borred == "1").map(book => (userId, book.book_id))
      val df = BooksData.getDF(recomm_record, spark)
      // 写入mysql
      BooksData.toMysql(df)
    }

    sc.stop()

  }

  /**
    * 转化list
    * @param list
    * @return
    */
  def tranList(list: List[Book]): List[BookModel] = {
    val booksList = list
    val bms = booksList.map(book => {
      BookModel(book, book.borred)
    })
    bms
  }

  // 获得每个用户对应的不包含自己已经借阅书籍的数据集
  val getMyDataSet = (person: (String, List[BorrowRecord])) => {
    val dataSet = BooksData.makeDataSet(person._2, BooksData.getAllBooks(spark))
    val noBorrBooks = dataSet.filter(book => {
      book.borred == "0"
    })
    (person._1, noBorrBooks, dataSet)
  }

  def main(args: Array[String]): Unit = {
    start(3)
  }

}
