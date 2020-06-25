package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/3 21:11
  * @Description:
  * @Modified By: lenovo
  */
object Spark19_RDD_Operator6 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context: SparkContext = new SparkContext(conf)
    // glom => 将每个分区的数据转换为数组
    val rdd1: RDD[Int] = context.makeRDD(List(1,2,3,4),2)
    val rdd2: RDD[Array[Int]] = rdd1.glom()
    rdd2.foreach(
      data =>{
        println(data.mkString(","))
      }
    )
    context.stop()
  }
}
