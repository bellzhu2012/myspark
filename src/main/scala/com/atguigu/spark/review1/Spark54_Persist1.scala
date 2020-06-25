package com.atguigu.spark.review1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/8 21:45
  * @Description:
  * @Modified By: lenovo
  */
object Spark54_Persist1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4))
    val mapRDD = rdd.map(num => {
      println("map------------")
      num
    })
    mapRDD.persist()
    println(mapRDD.toDebugString)
    val cacheRDD: RDD[Int] = mapRDD.cache()
    println(mapRDD.collect().mkString(","))
    println("++++++++++++++++++")
    println(cacheRDD.toDebugString)
    println(cacheRDD.collect().mkString("+"))
    sc.stop()
  }
}
