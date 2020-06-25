package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 9:59
  * @Description:
  * @Modified By: lenovo
  */
object Spark15_RDD_Operator44 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 获取每个分区最大值以及分区号
    val rdd = sc.makeRDD(List(1,2,6,4,5,3),3)
    val rdd1 = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        List((index, iter.max)).toIterator
      }
    )
    println(rdd1.collect().mkString(","))

    sc.stop()
  }
}
