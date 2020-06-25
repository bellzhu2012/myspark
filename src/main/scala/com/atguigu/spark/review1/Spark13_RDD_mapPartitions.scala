package com.atguigu.spark.review1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 13:41
  * @Description:
  * @Modified By: lenovo
  */
object Spark13_RDD_mapPartitions {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    // TODO Spark - RDD - 算子（方法）
    // mapPartitions
    // 以分区为单位进行计算， 和map算子很像
    // 区别就在于map算子是一个一个执行，而mapPartitions一个分区一个分区执行
    // 类似于批处理

    // map方法是全量数据操作，不能丢失数据
    // mapPartitions 一次性获取分区的所有数据，那么可以执行迭代器集合的所有操作
    //               过滤，max,sum
    val rdd1 = rdd.mapPartitions(
      iter => iter.map(_ * 2)
    )
    println(rdd1.collect().mkString(","))
    val rdd2: RDD[Int] = rdd.mapPartitions(
      iter => iter.filter(_ % 2 == 0)
    )
    println(rdd2.collect().mkString(","))
    sc.stop()
  }
}
