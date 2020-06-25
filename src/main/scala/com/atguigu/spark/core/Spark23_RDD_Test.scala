package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 21:43
  * @Description:
  * @Modified By: lenovo
  */
object Spark23_RDD_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 根据单词首字符进行分组
    val rdd = sc.makeRDD(List("hello","hive","hbase","Hadoop"),2)
    val data = rdd.groupBy(
      word => {
        word(0)
      }
    )
    println(data.collect().mkString(","))
    sc.stop()
  }
}
