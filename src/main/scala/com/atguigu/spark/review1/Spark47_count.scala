package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 20:18
  * @Description:
  * @Modified By: lenovo
  */
object Spark47_count {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4))
    val num: Long = rdd.count()
    val first: Int = rdd.first()
    val take = rdd.take(2)
    println(num)
    println(first)
    println(take.mkString(","))
    sc.stop()
  }
}
