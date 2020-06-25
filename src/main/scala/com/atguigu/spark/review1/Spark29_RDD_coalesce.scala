package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 17:41
  * @Description:
  * @Modified By: lenovo
  */
object Spark29_RDD_coalesce {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,1,1,2,2,2),2)
    val rdd1 = rdd.coalesce(6)
    rdd1.saveAsTextFile("output")
    sc.stop()
  }
}
