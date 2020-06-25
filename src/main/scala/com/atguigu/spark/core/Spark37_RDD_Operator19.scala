package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 14:47
  * @Description:
  * @Modified By: lenovo
  */
object Spark37_RDD_Operator19 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val context = new SparkContext(conf)
    val tuples: List[(String, Int)] = List(
      ("hello", 1), ("hello", 2), ("spark", 1), ("spark", 1)
    )

    val rdd: RDD[(String, Int)] = context.makeRDD(tuples)
    val rdd1 = rdd.reduceByKey(_+_)

    println(rdd1.collect().mkString(","))
    context.stop()
  }
}
