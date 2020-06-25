package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 9:54
  * @Description:
  * @Modified By: lenovo
  */
object Spark23_RDD_Sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val context = new SparkContext(conf)
    val rdd = context.makeRDD(List(1,2,3,4,5,6))
    val rdd1 = rdd.sample(false, 0.5)
//    val rdd1 = rdd.sample(true, 2)
    println(rdd1.collect().mkString(","))
    context.stop()
  }
}
