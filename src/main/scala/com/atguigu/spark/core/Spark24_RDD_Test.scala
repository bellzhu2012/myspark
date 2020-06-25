package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 21:46
  * @Description:
  * @Modified By: lenovo
  */
object Spark24_RDD_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("input/apache.log")
    val rdd1 = rdd.map(
      log => {
        val words = log.split(" ")
        words(3)
      }
    )
    val rdd2: RDD[(String, Iterable[String])] = rdd1.groupBy(
      time => {
        time.substring(11, 13)
      })

    sc.stop()
  }
}
