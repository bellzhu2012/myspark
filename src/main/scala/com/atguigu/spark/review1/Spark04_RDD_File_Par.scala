package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 13:15
  * @Description:
  * @Modified By: lenovo
  */
object Spark04_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("input/wordcount.txt",4)
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
