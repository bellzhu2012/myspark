package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}


/**
  * @Author: lenovo
  * @Time: 2020/6/5 22:04
  * @Description:
  * @Modified By: lenovo
  */
object Spark28_RDD_oPERATOR10 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,1,2,4))
    // 去重
    val rdd1 = rdd.distinct()
    // 设定分区，会导致shuffle的产生，数据被打乱
    val rdd2 = rdd.distinct(2)
    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))
    sc.stop()
  }
}
