package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/3 14:42
  * @Description:
  * @Modified By: lenovo
  */
object Spark09_RDD_Operator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val context: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = context.makeRDD(List(1,2,3,4))
    // 旧RDD->算子->新RDD
    val rdd2: RDD[Int] = rdd1.map(_*2)
    // collect方法不会转换RDD，但是会触发作业执行
    //又称为行动算子（action）
    val array: Array[Int] = rdd2.collect()

    context.stop()
  }
}
