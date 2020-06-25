package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
  * @Author: lenovo
  * @Time: 2020/6/22 12:26
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming02_WordCount11 {
  def main(args: Array[String]): Unit = {
    // SparkStreaming使用核数最少是两个，
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 从socket中获取数据，是一行一行获取的
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val wordDS = socketDS.flatMap(_.split(" "))
    val wordToOneDS = wordDS.map((_,1))
    val wordToSumDS = wordToOneDS.reduceByKey(_+_)

    wordToSumDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
