package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/3 21:32
  * @Description:
  * @Modified By: lenovo
  */
object Spark02_RDD_File11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("File")
    val context = new SparkContext(conf)
    // path:相对路径，绝对路径，第三方路径（hdfs），通配符
//    val rdd = context.textFile("input")
    // 通配符
    val rdd = context.textFile("input/wordcount*.txt")
    println(rdd.collect().mkString(","))
    context.stop()
  }
}
