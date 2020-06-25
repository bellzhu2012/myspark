package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/3 8:10
  * @Description:
  * @Modified By: lenovo
  */
object Spark03_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
    val context = new SparkContext(conf)
    // 在资源充足的情况下，RDD中的分区数量其实就是并行度
    // 设定并行度，其实就在设定分区数
    // 1 makeRDD的第一个参数：数据源
    // 2 makeRDD的第二个参数：默认并行度（分区的数量）
    //  numSlices: Int = defaultParallelism（默认并行度）
    // scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // 并行度默认会从spark配置信息中获取spark.default.parallelism值
    // 如果获取不到该参数，则会采用默认值totalCores（机器总核数）
    // 机器总核数 = 当前环境中可用核数
    // local => 单核数（单线程）=>1
    // local[4] => 4核（4个线程）=>4
    // local[*] => 机器最大核数（机器最多线程）=>8
    val rdd: RDD[Int] = context.makeRDD(List(1,2,3,4),3)
    rdd.saveAsTextFile("output1")

    context.stop()
  }
}
