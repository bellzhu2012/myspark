package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 18:48
  * @Description:
  * @Modified By: lenovo
  */
object Spark38_RDD_groupByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(
      ("hello", 1), ("hello", 3), ("hola", 1)
    ))
    val rdd1 = rdd.groupByKey()
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
