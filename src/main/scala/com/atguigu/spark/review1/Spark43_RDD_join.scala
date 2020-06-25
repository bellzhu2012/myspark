package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 20:06
  * @Description:
  * @Modified By: lenovo
  */
object Spark43_RDD_join {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(
      List(
        ("a", 1), ("b", 2), ("c", 3)
      ))
    val rdd2 = sc.makeRDD(
      List(
        ("a", 3), ("b", 1), ("c", 2)
      ))
    val rdd3 = rdd1.join(rdd2)
    println(rdd3.collect().mkString(","))
    sc.stop()
  }
}
