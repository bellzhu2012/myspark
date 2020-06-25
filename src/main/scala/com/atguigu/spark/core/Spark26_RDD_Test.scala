package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 21:54
  * @Description:
  * @Modified By: lenovo
  */
object Spark26_RDD_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("input/apache.log")
    val rdd1 = rdd.map(
      line => {
        val word = line.split(" ")
        word(3)
      }
    )
    val rdd2 = rdd1.filter(time => {
      val str = time.substring(0, 10)
      str == "17/05/2015"
    })
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
