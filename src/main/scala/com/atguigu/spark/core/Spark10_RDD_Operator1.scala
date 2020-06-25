package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/3 14:50
  * @Description:
  * @Modified By: lenovo
  */
object Spark10_RDD_Operator1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val context: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = context.makeRDD(List(1,2,3,4),2)
    // TODO 分区的问题
    //  旧RDD->算子->新RDD
    val rdd2: RDD[Int] = rdd1.map(_*2)
    rdd2.saveAsTextFile("output2")
    context.stop()
  }
}
