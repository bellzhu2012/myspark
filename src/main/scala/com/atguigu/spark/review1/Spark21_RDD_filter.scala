package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 14:41
  * @Description:
  * @Modified By: lenovo
  */
object Spark21_RDD_filter {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8),4)
    val rdd1 = rdd.filter(a => a % 2 == 0)
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
