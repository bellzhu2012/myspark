package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 13:12
  * @Description:
  * @Modified By: lenovo
  */
object Spark03_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4),2)

//    rdd.saveAsObjectFile("output")
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
