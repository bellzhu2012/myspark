package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 21:42
  * @Description:
  * @Modified By: lenovo
  */
object Spark44_RDD_Operator27 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("b", 2), ("c", 3), ("d", 4),("a",2)
      )
    )
    val rdd2: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 4), ("b", 5), ("c", 6)
      )
    )
    // cogroup：第一个RDD内的tuple，相同key的value形成集合，不同rdd的集合形成元组
    val result: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd2.cogroup(rdd1)
    result.collect().foreach(println)
    sc.stop()
  }
}
