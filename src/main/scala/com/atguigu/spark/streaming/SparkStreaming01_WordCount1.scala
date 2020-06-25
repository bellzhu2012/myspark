package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/15 20:23
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming01_WordCount1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    val wordDS : DStream[String] = socketDS.flatMap(_.split(" "))
    val wordToOneDS: DStream[(String, Int)] = wordDS.map((_,1))
    val wordToSumDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_+_)

    wordToSumDS.print()

    ssc.start()

//    ssc.stop()

    // 等待采集器的结束
    ssc.awaitTermination()
  }
}
