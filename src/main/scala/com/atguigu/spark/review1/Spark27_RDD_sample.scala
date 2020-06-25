package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 15:00
  * @Description:
  * @Modified By: lenovo
  */
object Spark27_RDD_sample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4,5))
    val rdd1 = rdd.sample(true, 0.5)
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
