package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 10:07
  * @Description:
  * @Modified By: lenovo
  */
object Spark17_RDD_Operator5 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(List(1,2),List(3,4)))
    // flatMap算子
    val rdd1 = rdd.flatMap(list => list)
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
