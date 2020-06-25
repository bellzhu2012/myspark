package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 17:55
  * @Description:
  * @Modified By: lenovo
  */
object Spark32_RDD_sortBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,3,2,4))
    val rdd1 = rdd.sortBy(num => num, false)
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
