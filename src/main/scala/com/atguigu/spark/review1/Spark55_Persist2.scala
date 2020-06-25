package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/8 21:54
  * @Description:
  * @Modified By: lenovo
  */
object Spark55_Persist2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")
    val rdd = sc.makeRDD(List(1,2,3,4))
    val mapRDD = rdd.map(num => {
      println("map------------")
      num
    })
    println(mapRDD.toDebugString)
//    val cacheRDD: RDD[Int] = mapRDD.cache()
//    cacheRDD.checkpoint()
    mapRDD.checkpoint()
    val cacheRDD: RDD[Int] = mapRDD.cache()
    println(cacheRDD.collect().mkString(","))
    println("++++++++++++++++++")
    println(cacheRDD.toDebugString)
    println(cacheRDD.collect().mkString("+"))
    sc.stop()
  }
}
