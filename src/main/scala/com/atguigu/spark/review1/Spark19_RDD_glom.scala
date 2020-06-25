package com.atguigu.spark.review1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 14:34
  * @Description:
  * @Modified By: lenovo
  */
object Spark19_RDD_glom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    val rdd1: RDD[Array[Int]] = rdd.glom()
    rdd1.collect().foreach(elem => println(elem.mkString(",")))
    sc.stop()
  }
}
