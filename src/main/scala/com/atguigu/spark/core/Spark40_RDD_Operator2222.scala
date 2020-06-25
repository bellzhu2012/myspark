package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 8:20
  * @Description:
  * @Modified By: lenovo
  */
object Spark40_RDD_Operator2222 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("c", 3),
        ("b", 4), ("c", 5), ("c", 6)
      )
      , 2
    )

    // 分区间规则和分区内规则一样，可以用foldByKey代替aggregateByKey
//    val rdd1 = rdd.aggregateByKey(0)(
//      (x, y) => {
//        x + y
//      },
//      (x, y) => {
//        x + y
//      }
//    )
    val rdd1 = rdd.foldByKey(0)(_ + _)

    sc.stop()
  }
}
