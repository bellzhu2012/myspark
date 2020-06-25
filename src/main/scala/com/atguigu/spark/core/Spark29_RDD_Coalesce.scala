package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 10:58
  * @Description:
  * @Modified By: lenovo
  */
object Spark29_RDD_Coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val context = new SparkContext(conf)
    val rdd = context.makeRDD(List(1,3,5,7,2,4,6,8),4)
    val rdd1 = rdd.filter( _ % 2 == 0)
//    rdd1.saveAsTextFile("output")
    val rdd2 = rdd1.coalesce(2,true)
    rdd2.saveAsTextFile("output1")
    context.stop()
  }
}
