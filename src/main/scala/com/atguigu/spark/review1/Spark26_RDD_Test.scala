package com.atguigu.spark.review1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 14:55
  * @Description:
  * @Modified By: lenovo
  */
object Spark26_RDD_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("input/apache.log")
    val rdd1 = rdd.map(log => {
      val words = log.split(" ")
      words(3)
    })
    val rdd2 = rdd1.filter(data => {
      val pattern = "17/05/2015".r
      val maybeString: Option[String] = pattern.findFirstIn(data)
      if (maybeString.getOrElse(0).isInstanceOf[String]) {
        true
      } else {
        false
      }
    })
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
