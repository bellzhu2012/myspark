package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @Author: lenovo
  * @Time: 2020/6/22 12:37
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming04_File1 {
  def main(args: Array[String]): Unit = {
    // SparkStreaming使用核数最少是两个，
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 通过文件创建DS
    val dirDS = ssc.textFileStream("in")
    dirDS.print()
    ssc.start()


    ssc.awaitTermination()
  }
}
