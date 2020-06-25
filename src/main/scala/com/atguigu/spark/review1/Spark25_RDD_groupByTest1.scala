package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 14:52
  * @Description:
  * @Modified By: lenovo
  */
object Spark25_RDD_groupByTest1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List("hello spark", "hello scala","hello"),3)
    println(rdd
      .flatMap(_.split(" "))
      .groupBy(key => key)
      .map(kv => (kv._1, kv._2.size))
      .collect().mkString(","))

    sc.stop()
  }
}
