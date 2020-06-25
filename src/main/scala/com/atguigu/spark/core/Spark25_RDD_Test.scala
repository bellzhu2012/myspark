package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 21:50
  * @Description:
  * @Modified By: lenovo
  */
object Spark25_RDD_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val dataRDD = sc.makeRDD(List("hello scala", "hello"))
    val rdd = dataRDD
      .flatMap(_.split(" "))
      .groupBy(word => word)
      .map(kv => (kv._1, kv._2.size))
    println(rdd.collect().mkString(","))
    sc.stop()
  }
}
