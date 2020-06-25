package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/3 15:32
  * @Description:
  * @Modified By: lenovo
  */
object Spark12_RDD_Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val context: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = context.textFile("input/apache.log")
    // 旧RDD->算子->新RDD
    val rdd1 = rdd.map(
      (line) => {
        val strings: Array[String] = line.split(" ")
        val length = strings.length
        strings(length - 1)
      })

    rdd1.collect().foreach(println)

    context.stop()
  }
}
