package com.atguigu.spark.review1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 20:22
  * @Description:
  * @Modified By: lenovo
  */
object Spark48_takeOrdered {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,3,2,4))
    val rdd1 = rdd.takeOrdered(3)
    println(rdd1.mkString(","))
    sc.stop()
  }
}
