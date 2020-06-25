package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/3 21:38
  * @Description:
  * @Modified By: lenovo
  */
object Spark03_RDD_Memory_Par22 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("File")
    val context = new SparkContext(conf)
    // 从内存中创建RDD
    // makeRDD的分区的数量就是并行度，设定并行度，其实就是制定分区数量
    // 1 makeRDD的第一个参数：数据源
    // 2 makeRDD的第二个参数：默认并行度（分区数量）
//    numSlices: Int = defaultParallelism（默认并行度）
    // scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // 如果没有设定默认并行度，就采用机器总核数
    // 机器的总核数 = 当前环境的可用核数
    // local => 单核（单线程）->1
    // local[4] => 4核数（4个线程）->4
    // local[*] => 最大核数->8

    val rdd = context.makeRDD(List(1,2,3,4),3)
//    val start = ((i * length) / numSlices).toInt
//    val end = (((i + 1) * length) / numSlices).toInt
    // slice(from : Int, until : Int)

//    println(rdd.collect().mkString(","))
    // 将RDD的处理后的数据保存到分区文件
    rdd.saveAsTextFile("output")
    context.stop()
  }
}