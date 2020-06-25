package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 9:44
  * @Description:
  * @Modified By: lenovo
  */
object Spark12_RDD_Test11 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 从服务器日志数据apache.log中获取用户请求URL资源路径
    val rdd = sc.textFile("input/apache.log")
    val rdd1 = rdd.map(line => {
      val words = line.split(" ")
      words(words.length - 1)
    })
    rdd1.collect().foreach(println)
    sc.stop()
  }
}
