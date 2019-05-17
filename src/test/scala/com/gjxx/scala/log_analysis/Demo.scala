package com.gjxx.scala.log_analysis

import com.gjxx.scala.log_analysis.BooksData.Book

object Demo {

  def main(args: Array[String]): Unit = {

    val b1 = BookModel(Book("1", "aa", "au1", "pr1", "ca1", "0"), "0")
    val b2 = BookModel(Book("2", "bb", "au2", "pr2", "ca2", "0"), "0")
    val b3 = BookModel(Book("3", "cc", "au3", "pr3", "ca3", "1"), "1")
    val b4 = BookModel(Book("4", "dd", "au4", "pr4", "ca4", "1"), "1")
    val b5 = BookModel(Book("5", "ee", "au5", "pr5", "ca5", "0"), "0")
    val b6 = BookModel(Book("6", "ff", "au6", "pr6", "ca6", "1"), "1")
    b1.distance = 2
    b2.distance = 1
    b3.distance = 1
    b4.distance = 1
    b5.distance = 4
    b6.distance = 2

    val list = List(b1, b2, b3, b4, b5, b6)

    val l2 = list.sortWith((bm1, bm2) => {
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

    l2.foreach(println(_))
    println("====================")

    list.sortBy(_.distance).foreach(println(_))

  }

}
