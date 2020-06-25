package com.atguigu.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/2 8:49
  * @Description:
  * @Modified By: lenovo
  */
object WordCount111 {
  def main(args: Array[String]): Unit = {
    // 准备spark配置环境
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    // 准备spark连接
    val context = new SparkContext(conf)
    // 业务逻辑
    val lineRDD: RDD[String] = context.textFile("input")
    // 数据切割
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    // 数据映射
    val tupleRDD = wordRDD.map(word => (word,1))
    // 聚合统计
    val wordToCountRDD = tupleRDD.reduceByKey(_ + _)
    // 结果收集
    val array = wordToCountRDD.collect()
    // 查看
    println(array.mkString(","))
    // 释放资源
    context.stop()
  }
}
