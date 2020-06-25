package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 21:54
  * @Description:
  * @Modified By: lenovo
  */
object Spark22_RDD_Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("filter")
    val context = new SparkContext(conf)
    val rdd = context.textFile("input/apache.log")
    val rdd1 = rdd.filter(
      line => {
        val words = line.split(" ")
        val pattern = "17/05/2015.*".r
        pattern.findFirstIn(words(3)).getOrElse(0).isInstanceOf[String]
      }
    )
    val rdd2 = rdd1.map(line => {
      val words = line.split(" ")
      words(3) + "==>" + words(words.length - 1)
    })
    rdd2.collect().foreach(println)
    context.stop()
  }
}
