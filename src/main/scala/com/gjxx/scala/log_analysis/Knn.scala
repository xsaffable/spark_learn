package com.gjxx.scala.log_analysis

import com.gjxx.scala.log_analysis.BooksData.Book

/**
  * knn算法实现
  */
object Knn {

  /**
    * 根据距离排序
    * @param bookModels
    * @return
    */
  def sortByDistance(bms: List[BookModel]): List[BookModel] = {
    bms.sortWith((bm1, bm2) => {
      if (bm1.distance < bm2.distance) {
        true
      } else if (bm1.distance == bm2.distance) {
        if (bm1.bookType > bm2.bookType) {
          true
        } else {
          false
        }
      } else {
        false
      }
    })
  }

  /**
    * 计算k到每个样本的距离
    * 出版社、作者、分类计算，其比重为1:2:4
    * @param bookModels
    * @param bookModel
    */
  def computeDistance(bookModels: List[BookModel], bookModel: BookModel): List[BookModel] = {
    bookModels.map(bm => {
      val arr = Array.fill(3){0}
      var count: Double = 0
      if (bm.book.press == bookModel.book.press) {
        arr(0) = 0
      } else {
        arr(0) = 1
      }
      if (bm.book.author == bookModel.book.author) {
        arr(1) = 0
      } else {
        arr(1) = 1
      }
      if (bm.book.category == bookModel.book.category) {
        arr(2) = 0
      } else {
        arr(2) = 1
      }
      for (i <- arr.indices) {
        count += arr(i) * Math.pow(2, i)
      }
      bm.distance = count
      bm
    })
  }

  /**
    * 获取指定范围中存在最多的数据
    * @param kms
    * @return
    */
  def findMostValue(bms: List[BookModel]): String = {
    var typeCountMap: Map[String, Int] = Map()
    for (i <- bms.indices) {
      typeCountMap += (bms(i).bookType -> (typeCountMap.getOrElse(bms(i).bookType, 0) + 1))
    }
    typeCountMap.toList.minBy(_._2)._1
  }

  /**
    * 程序入口
    * @param bms 样本数据集
    * @param bm 待测试数据
    * @param k
    * @return
    */
  def run(bms: List[BookModel], bm: BookModel, k: Int): String = {
    val bms2 = computeDistance(bms, bm)
    val knns = sortByDistance(bms2).take(k)
    findMostValue(knns)
  }

}

case class BookModel(book_str: Book, bookType_str: String) {
  var book = book_str
  var bookType = bookType_str
  var distance: Double = 0
}
