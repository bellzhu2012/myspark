package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 8:28
  * @Description:
  * @Modified By: lenovo
  */
object Spark08_RDD_File_PartitionData2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // hadoop 分区是以文件为单位进行划分的
    // 读取数据不能跨越文件

    // totalSize = 12, numSplits = 3
    // goalSize = totalSize / numSplits = 12/3 = 4byte
    // a.txt
    //      0 -> (0,4) ->1@@ 234
    //      1 -> (4,8) -> 空
    // b.txt
    //      0 -> (0,4) -> 5@@ 678
    //      1 -> (4,8) -> 空
    val rdd = sc.textFile("input1",3)
    rdd.saveAsTextFile("output2")
    sc.stop()
  }
}
