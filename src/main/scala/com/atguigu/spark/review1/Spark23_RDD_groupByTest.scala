package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 14:48
  * @Description:
  * @Modified By: lenovo
  */
object Spark23_RDD_groupByTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List("hello","hive","hbase","Hadoop"))
    val rdd1 = rdd.groupBy(key => key(0))
    println(rdd1.collect().mkString(","))

    sc.stop()
  }
}
