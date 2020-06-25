package com.atguigu.spark.review1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 17:59
  * @Description:
  * @Modified By: lenovo
  */
object Spark33_RDD_doubleValue {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(List(1,3,2,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))
    // 双value型操作
    // 合集
    val unionRDD: RDD[Int] = rdd1.union(rdd2)
    // 交集
    val intersectRDD: RDD[Int] = rdd1.intersection(rdd2)
    // 差集
    val subRDD: RDD[Int] = rdd1.subtract(rdd2)
    // 拉链
    val zipRDD = rdd1.zip(rdd2)

    println(unionRDD.collect().mkString(","))
    println(intersectRDD.collect().mkString(","))
    println(subRDD.collect().mkString(","))
    println(zipRDD.collect().mkString(","))



    sc.stop()
  }
}
