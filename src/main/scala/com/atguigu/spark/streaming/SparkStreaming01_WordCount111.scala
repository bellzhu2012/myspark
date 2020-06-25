package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/22 11:28
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming01_WordCount111 {
  def main(args: Array[String]): Unit = {
    // SparkStreaming 环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("local")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    // 从socket获取数据，一行一行获取
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val wordDS: DStream[String] = socketDS.flatMap(_.split(" "))
    val wordToOneDS: DStream[(String, Int)] = wordDS.map((_,1))
    val wordToSumDS = wordToOneDS.reduceByKey(_ + _)

    wordToSumDS.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
