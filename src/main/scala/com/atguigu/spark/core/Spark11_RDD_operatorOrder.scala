package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/3 15:04
  * @Description:
  * @Modified By: lenovo
  */
object Spark11_RDD_operatorOrder {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val context: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = context.makeRDD(List(1,2,3,4),2)
    // 旧RDD->算子->新RDD
    val rdd2: RDD[Int] = rdd1.map(x=>{
      println("map 1 = " + x)
      x
    })
    val rdd3: RDD[Int] = rdd2.map(x=>{
      println("map 2 = " + x)
      x
    })

    println(rdd3.collect().mkString("-"))

    context.stop()
  }
}
