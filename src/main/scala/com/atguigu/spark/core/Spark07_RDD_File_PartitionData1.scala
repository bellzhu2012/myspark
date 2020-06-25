package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 8:21
  * @Description:
  * @Modified By: lenovo
  */
object Spark07_RDD_File_PartitionData1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 从磁盘中创建RDD
    // 1 分几个区
    // 10 byte / 4 = 2byte ... 2byte => 实际5分区
    //偏移量计算
    // 0 -> (0,2)
    // 2 -> (2,4)
    // 4 -> (4,6)
    // 6 -> (6,8)
    // 8 -> (8,10)
    // 2 数据是如何存储的？
    // 数据是以行的方式读取，但是会考虑偏移量（数据的offset）的设置，同时已经读取的数据不再重复读取
    // 1@@ => 012
    // 2@@ => 345
    // 3@@ => 678
    // 4 => 9

    // 实际分区存储
    // 0 -> (0,2) => 1@@
    // 2 -> (2,4) => 2@@
    // 4 -> (4,6) => 3@@
    // 6 -> (6,8) => 前面已经读取，此分区为空
    // 8 -> (8,10) =>4
    val rdd = sc.textFile("input/file.txt",4)
    rdd.saveAsTextFile("output1")
    sc.stop()
  }
}
