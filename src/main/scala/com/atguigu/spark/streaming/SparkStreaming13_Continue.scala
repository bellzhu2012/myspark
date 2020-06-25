package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * @Author: lenovo
  * @Time: 2020/6/22 10:09
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming13_Continue {
  def main(args: Array[String]): Unit = {
   val ssc = getStreamingContext()
    ssc.start()

    // 等待采集器结束
    ssc.awaitTermination()
  }
  def getStreamingContext(): StreamingContext = {
    // SparkStreaming使用核数最少是两个，
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    // SparkStreaming中的检查点不仅仅保存中间处理数据，还保存逻辑
    ssc.checkpoint("cp")
    val ris: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    ris.print()
    ssc
  }
}
