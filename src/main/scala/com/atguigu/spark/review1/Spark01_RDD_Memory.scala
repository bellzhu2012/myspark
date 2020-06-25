package com.atguigu.spark.review1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 13:08
  * @Description:
  * @Modified By: lenovo
  */
object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 从内存中创建RDD
    val list = List(1,2,3,4)
    val rdd: RDD[Int] = sc.parallelize(list)
    println(rdd.collect().mkString(","))
    sc.stop()
  }
}
