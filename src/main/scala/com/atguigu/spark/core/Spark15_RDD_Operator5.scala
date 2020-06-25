package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/3 16:09
  * @Description:
  * @Modified By: lenovo
  */
object Spark15_RDD_Operator5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val context: SparkContext = new SparkContext(conf)
    val rdd = context.makeRDD(List(1,2,3,4),2)
    // 旧RDD->算子->新RDD
    val rdd1 = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        List((index, iter.max)).toIterator
      }
    )
    rdd1.collect().foreach(println)

    context.stop()
  }
}
