package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 21:36
  * @Description:
  * @Modified By: lenovo
  */
object Spark43_RDD_Operator25 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(
      List(
        ("a", 1), ("b", 2), ("a", 3)
      )
    )
    val rdd2 = sc.makeRDD(
      List(
        ("a", 6), ("a", 2), ("b", 2)
      )
    )
    // join方法可以将两个rdd中相同的key的value连在一起
    // join方法性能不太高，尽量不要使用（可能会出现笛卡尔积）
    val rdd3 = rdd1.join(rdd2)
    println(rdd3.collect().mkString(","))
    sc.stop()
  }
}
