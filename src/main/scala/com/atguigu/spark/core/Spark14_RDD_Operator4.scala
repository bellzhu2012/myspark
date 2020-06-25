package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/3 15:50
  * @Description:
  * @Modified By: lenovo
  */
object Spark14_RDD_Operator4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val context: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = context.makeRDD(List(1,5,2,3,4,6),2)
    // 旧RDD->算子->新RDD
    val rdd1 = rdd.mapPartitions(
      iter => {
        List(iter.max).toIterator
      }
    )

    rdd1.collect().foreach(println)

    context.stop()
  }
}
