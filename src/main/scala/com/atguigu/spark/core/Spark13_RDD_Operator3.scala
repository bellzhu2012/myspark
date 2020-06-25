package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/3 15:44
  * @Description:
  * @Modified By: lenovo
  */
object Spark13_RDD_Operator3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val context: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = context.makeRDD(List(1,2,3,4),2)
    // 旧RDD->算子->新RDD
    // 分区转换
//    val rdd2 = rdd1.mapPartitions(
//      iter => {
//        iter.map(_ * 2)
//      }
//    )
    // 分区过滤
    val rdd2 = rdd1.mapPartitions(
            iter => {
              iter.filter(_%2 == 0)
            }
          )
    println(rdd2.collect().mkString("-"))
    context.stop()
  }
}
