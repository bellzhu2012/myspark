package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/15 21:43
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming04_File {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    val dirDS: DStream[String] = ssc.textFileStream("in")
    // 监控相对路径下的in目录的文件，要求文件在程序运行之后创建
    // 该方法不稳定
    dirDS.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
