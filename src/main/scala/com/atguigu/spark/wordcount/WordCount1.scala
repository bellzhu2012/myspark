package com.atguigu.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/1 17:16
  * @Description:
  * @Modified By: lenovo
  */
object WordCount1 {
  def main(args: Array[String]): Unit = {
    // 准备spark环境
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    // 准备spark连接
    val context: SparkContext = new SparkContext(conf)
    // 代码逻辑
    // 获取文件数据
    val words: RDD[String] = context.textFile("input")
    // 切割数据
    val wordRDD: RDD[String] = words.flatMap(_.split(" "))
    // 映射word->（word,1）
    val tupleRDD: RDD[(String, Int)] = wordRDD.map(word=>(word,1))
    // 分组聚合
    val result: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)
    // 采集结果
    val finalResult = result.collect()
    println(finalResult.mkString(","))
    // 释放资源
    context.stop()
  }
}
