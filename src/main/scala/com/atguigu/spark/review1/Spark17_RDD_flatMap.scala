package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/7 14:27
  * @Description:
  * @Modified By: lenovo
  */
object Spark17_RDD_flatMap {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(
      List(1, 2), List(3, 4)
    ))
    val rdd1 = rdd.flatMap(list => list)
    println(rdd1.collect().mkString(","))

    sc.stop()
  }
}
