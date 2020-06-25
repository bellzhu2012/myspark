package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 20:14
  * @Description:
  * @Modified By: lenovo
  */
object Spark45_RDD_collect {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4))
    val data: Array[Int] = rdd.collect()
    data.foreach(println)
    sc.stop()
  }
}
