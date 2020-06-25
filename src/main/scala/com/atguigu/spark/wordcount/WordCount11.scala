package com.atguigu.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/1 18:31
  * @Description:
  * @Modified By: lenovo
  */
object WordCount11 {
  def main(args: Array[String]): Unit = {
    // 准备配置环境
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    // 准备spark连接
    val context: SparkContext = new SparkContext(conf)
    // 业务逻辑
    // 读取文件数据
    val lineRDD: RDD[String] = context.textFile("input")
    // 切割数据
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    // 映射 word=》（word，1）
    val tupleRDD: RDD[(String, Int)] = wordRDD.map(word=>(word,1))
    // reduce
    val result: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)
    // 收集结果
    val finalResult = result.collect()
    // 打印结果
    println(finalResult.mkString(","))
    // 释放资源
  }
}
