package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 13:10
  * @Description:
  * @Modified By: lenovo
  */
object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("input/wordcount.txt")
    println(rdd.collect().mkString(","))
    sc.stop()
  }
}
