package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 10:12
  * @Description:
  * @Modified By: lenovo
  */
object Spark18_RDD_Test11 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 将List（List（1,2），3，List（4,5））进行扁平化操作
    val data = List(List(1, 2), 3, List(4, 5))
    val rdd = sc.makeRDD(
      data
    )
    val rdd1 = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case a: Int => List(a)
        }
      }
    )
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
