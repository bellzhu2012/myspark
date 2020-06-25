package com.atguigu.spark.review1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 13:28
  * @Description:
  * @Modified By: lenovo
  */
object Spark10_RDD_Map {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    val rdd1: RDD[Int] = rdd.map(_ * 2)
    // map算子：默认分区数量不变，数据会转换后输出
    // TODO 分区内数据是按照顺序依次执行，第一条数据所有的逻辑全部执行完毕后才会执行下一条数据
    //      分区间数据执行没有顺序，而且无需等待
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
