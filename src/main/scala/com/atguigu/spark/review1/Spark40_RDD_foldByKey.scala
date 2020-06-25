package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 19:09
  * @Description:
  * @Modified By: lenovo
  */
object Spark40_RDD_foldByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // reduceByKey : 分区内和分区间计算规则相同
    // 如果分区间和分区内规则不一样
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("c", 3),
      ("b", 4), ("c", 5), ("c", 6)
    ),2)
//    val rdd1 = rdd.aggregateByKey(10)(
//      (x, y) => x+y,
//      (x, y) => x + y
//    )
    val rdd1 = rdd.foldByKey(10)(_ + _)
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
