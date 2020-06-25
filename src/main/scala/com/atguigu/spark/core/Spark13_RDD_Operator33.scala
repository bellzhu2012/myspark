package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 9:49
  * @Description:
  * @Modified By: lenovo
  */
object Spark13_RDD_Operator33 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    //mapPartitions
    // 以分区为单位进行计算，和map算子很像
    // 区别：map算子是一个一个执行，而mapPartitions是一个分区一个分区执行
    // 类似批处理

    // map方法是全量数据操作，不能丢失数据
    // mapPartitions一次性获取分区的所有数据，那么可以执行迭代器集合的所有操作，如过滤，max和sum等
    val dataRDD = sc.makeRDD(List(1,2,3,4),2)
//    val rdd = dataRDD.mapPartitions(
//      iter => iter.map(_ * 2)
//    )
    val rdd = dataRDD.mapPartitions(
      iter => iter.filter(_ % 2 == 0)
    )

    println(rdd.collect().mkString(","))

    sc.stop()
  }
}
