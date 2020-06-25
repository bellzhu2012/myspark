package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/13 11:40
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val stream = new StreamingContext(conf, Seconds(3))
    // 执行逻辑
    val socketDS: ReceiverInputDStream[String] = stream.socketTextStream("localhost",9999)
    val wordDS = socketDS.flatMap(_.split(" "))
    val wordToCountList = wordDS.map((_,1))
    val result = wordToCountList.reduceByKey(_+_)
    result.print()

    // Driver程序在数据处理的过程中不能关闭
//    stream.stop()
    // 启动采集器
    stream.start()
    // 等待采集器的结束
    stream.awaitTermination()
  }
}
