package com.atguigu.spark.review1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 14:45
  * @Description:
  * @Modified By: lenovo
  */
object Spark22_RDD_glomTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)
    val rdd1: RDD[Array[Int]] = rdd.glom()
    val rdd2 = rdd1.map(array => array.max)
    val result: Array[Int] = rdd2.collect()
    println(result.sum)
    sc.stop()
  }
}
