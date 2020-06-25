package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 18:41
  * @Description:
  * @Modified By: lenovo
  */
object Spark37_RDD_reduceByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(
      ("hello", 1), ("hello", 1), ("hello", 1)
    ))
    val rdd1 = rdd.reduceByKey(_ + _)
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
