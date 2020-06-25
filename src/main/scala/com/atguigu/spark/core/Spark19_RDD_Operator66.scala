package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 10:17
  * @Description:
  * @Modified By: lenovo
  */
object Spark19_RDD_Operator66 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    // 将每个分区的数据转换为数组
    val rdd1: RDD[Array[Int]] = rdd.glom()
    rdd1.foreach(
      array => println(array.mkString(","))
    )
    sc.stop()
  }
}
