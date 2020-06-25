package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 10:02
  * @Description:
  * @Modified By: lenovo
  */
object Spark16_RDD_Test11 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,3,6,2,5,4),3)
    val rdd1 = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        // 获取的分区索引从0开始
        if (index == 1) {
          iter
        } else {
          Nil.iterator // 空迭代器
        }
      })
    println(rdd1.collect().mkString(","))

    sc.stop()
  }
}
