package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/7 14:20
  * @Description:
  * @Modified By: lenovo
  */
object Spark15_RDD_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    // 获取每个分区的最大值，以及分区号
    val rdd1 = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        List(iter.max, index).toIterator
      }
    )
    println(rdd1.collect().mkString(","))

  }
}
