package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/3 16:18
  * @Description:
  * @Modified By: lenovo
  */
object Spark17_RDD_Operator6 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val context: SparkContext = new SparkContext(conf)
//    val rdd = context.makeRDD(List(List(1,2),List(3,4)),2)
    val rdd = context.makeRDD(List(List(1,2),List(3,4),5),2)
    // 旧RDD->算子->新RDD

    val rdd1 = rdd.flatMap(list => {
      list match {
        case a: List[_] => a
        case b: Int => List(b)
      }
    })
    rdd1.collect().foreach(println)

    context.stop()
  }
}
