package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 9:32
  * @Description:
  * @Modified By: lenovo
  */
object Spark11_RDD_Operator22 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    // 数据处理顺序问题
    // 分区内数据是按照顺序依次执行，第一条数据所有的逻辑全部执行完后，才会执行下一条数据
    // 分区间数据执行没有顺序，而且无需等待
    // 小结：分区内有序，分区间无序（分区内串行，分区间并行）
    // 注意数据的分区
    val rdd1 = rdd.map(x => {
      println("map 1 = " + x)
      x
    })
    val rdd2 = rdd1.map(x => {
      println("map 2 = " + x)
      x
    })
    println(rdd2.collect().mkString(","))
    sc.stop()
  }
}
