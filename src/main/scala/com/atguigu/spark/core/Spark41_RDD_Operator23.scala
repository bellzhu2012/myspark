package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 16:40
  * @Description:
  * @Modified By: lenovo
  */
object Spark41_RDD_Operator23 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val context = new SparkContext(conf)
    val rdd = context.makeRDD(List(("a",87),("a",92),("b",72),("b",81),("a",73),("b",77)))

//    println(rdd1.collect().mkString(","))
    context.stop()
  }
}
