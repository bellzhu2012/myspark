package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 21:38
  * @Description:
  * @Modified By: lenovo
  */
object Spark22_RDD_Test11 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 计算所有分区最大值求和（分区内去最大值，分区间求最大值和）
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    val maxRDD = glomRDD.map(array => array.max)
    val array = maxRDD.collect()
    println(array.sum)
    sc.stop()
  }
}
