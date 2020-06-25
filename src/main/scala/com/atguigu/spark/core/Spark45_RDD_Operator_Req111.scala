package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 21:50
  * @Description:
  * @Modified By: lenovo
  */
object Spark45_RDD_Operator_Req111 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 统计出每一个省份的每个广告被点击数量排行的Top3
    val rdd: RDD[String] = sc.textFile("input/agent.log")
    val rdd1 = rdd.map(
      line => {
        val words = line.split(" ")
        (words(1) + "-" + words(4), 1)
      }
    )
    val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)
    val rdd3 = rdd2.map {
      case (key, count) => {
        val keys = key.split("-")
        (keys(0), (keys(1), count))
      }
    }
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()
    val rdd5 = rdd4.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(3)
      }
    )
    rdd5.collect().foreach(println)

    sc.stop()
  }
}
