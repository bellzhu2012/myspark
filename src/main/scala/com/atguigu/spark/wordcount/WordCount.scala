package com.atguigu.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/1 16:22
  * @Description:
  * @Modified By: lenovo
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 准备spark运行环境
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    // 准备spark连接对象
    val sparkContext = new SparkContext(sparkConf)
    // 业务逻辑
    // 读取文件数据
    val fileRDD: RDD[String] = sparkContext.textFile("input")
    // 将文件中的数据进行分词
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    // 将数据进行分组
    val wordToListMapRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(key=>key)
    // map中value进行统计
    val wordToCountMapRDD: RDD[(String, Int)] = wordToListMapRDD.mapValues(value=>value.size)
    // 将数据结果采集到内存
    val result: Array[(String, Int)] = wordToCountMapRDD.collect()
    println(result.mkString(","))
    // 释放资源
    sparkContext.stop()
  }

}
