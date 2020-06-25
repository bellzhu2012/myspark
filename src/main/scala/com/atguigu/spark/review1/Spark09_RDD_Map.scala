package com.atguigu.spark.review1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 13:24
  * @Description:
  * @Modified By: lenovo
  */
object Spark09_RDD_Map {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    //    val rdd1: RDD[Int] = rdd.map((i)=>{ i * 2})
    val rdd1: RDD[Int] = rdd.map(_ *2)
    val rdd2: RDD[(Int, Int)] = rdd1.map((_,1))
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
