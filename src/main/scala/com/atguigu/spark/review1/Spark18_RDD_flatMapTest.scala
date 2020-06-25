package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/7 14:30
  * @Description:
  * @Modified By: lenovo
  */
object Spark18_RDD_flatMapTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(
      List(1, 2), 3, List(4, 5)
    ))
    val rdd1 = rdd.flatMap(
      list => list match {
        case a: List[_] => a
        case b: Int => List(b)
      }
    )
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
