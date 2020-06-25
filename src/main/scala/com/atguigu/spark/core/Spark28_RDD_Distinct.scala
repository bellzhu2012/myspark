package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 10:14
  * @Description:
  * @Modified By: lenovo
  */
object Spark28_RDD_Distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val context = new SparkContext(conf)
    val rdd = context.makeRDD(List(1,2,3,1,2,3),2)
    val rdd1 = rdd.distinct(1)
    rdd1.saveAsTextFile("output")
    context.stop()
  }
}
