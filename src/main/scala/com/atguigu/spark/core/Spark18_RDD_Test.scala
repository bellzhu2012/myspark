package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/3 21:05
  * @Description:
  * @Modified By: lenovo
  */
object Spark18_RDD_Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context: SparkContext = new SparkContext(conf)
    // 将List(List(1,2),3,List(4,5))进行扁平化处理
    val dataRDD: RDD[Any] = context.makeRDD(List(List(1,2),3,List(4,5)))
    val rdd = dataRDD.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case a: Int => List(a)
        }
      }
    )
    println(rdd.collect().mkString("-"))
    context.stop()
  }
}
