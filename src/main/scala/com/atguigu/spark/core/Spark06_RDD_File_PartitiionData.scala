package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 8:10
  * @Description:
  * @Modified By: lenovo
  */
object Spark06_RDD_File_PartitiionData {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 从磁盘中创建RDD
    // 1 spark读取文件采用的是hadoop的读取规则
    // 文件切片规则:以字节方式来切片
    // 数据读取规则:以行为单位来读取
    // 2 问题
    // 文件到底切成几片
    // 文件字节数（10），预计切片数量（2）--最小切片数
    // 10 / 2 = 5 bytes
    // totalSize = 10
    // goalsize = totalSize / numSplits = 10 / 2 = 5 => 2 分区
    // totalSize = 11
    // goalSize = totalSize / numSplits = 11 / 2 ...1 => 3 分区
    // 所谓的最小分区数，取决于总字节数是否能整除分区数并且剩余的字节达到一个比率
    // 实际产生的分区数量可能大于最小分区数

    // 分区数据如何存储？
    // 分区数据是以行为单位读取的，而不是以字节
    // 实际上是偏移量 + 行读取
    val rdd = sc.textFile("input/w.txt", 2)
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
