package com.gjxx.scala.saprk_mllib

import org.apache.spark.mllib.linalg.Vectors
import org.junit.Test

class Demo02 {

  @Test
  def test1(): Unit = {
    // 创建稠密向量
    val denseVec1 = Vectors.dense(1.0, 2.0, 3.0)
    val denseVector2 = Vectors.dense(Array(1.0, 2.0, 3.0))

    // 创建稀疏向量<1.0, 0.0, 2.0, 0.0>
    val sparseVec1 = Vectors.sparse(4, Array(0, 2), Array(1.0, 2.0))

  }

}

