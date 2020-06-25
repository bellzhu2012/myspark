package com.atguigu.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/1 18:24
  * @Description:
  * @Modified By: lenovo
  */
object WordCount00 {
  def main(args: Array[String]): Unit = {
    // 准备spark环境配置
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    // 准备spark连接
    val context: SparkContext = new SparkContext(conf)
    // 业务逻辑
    // 读取文件数据  textFile（相对路径）
    val lineRDD: RDD[String] = context.textFile("input")
    // 切割数据
    val wordRDD: RDD[String] = lineRDD.flatMap(line=>line.split(" "))
    // 数据分组
    val wordToListRDD = wordRDD.groupBy(word=>word)
    // 统计数据
    val wordToCountRDD: RDD[(String, Int)] = wordToListRDD.mapValues(value=>value.size)
    // 结果收集
    val result: Array[(String, Int)] = wordToCountRDD.collect()
    // 查看结果
    println(result.mkString(","))
    // 资源释放
  }
}
