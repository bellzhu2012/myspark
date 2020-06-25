package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 13:38
  * @Description:
  * @Modified By: lenovo
  */
object Spark12_RDD_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("input/apache.log")
    val rdd1 = rdd.map(
      line => {
        val words = line.split(" ")
        words(words.length - 1)
      }
    )
    rdd1.collect().foreach(println)

    sc.stop()
  }
}
