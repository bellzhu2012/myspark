package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * @Author: lenovo
  * @Time: 2020/6/22 8:11
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming09_Window {
  def main(args: Array[String]): Unit = {
    // SparkStreaming使用核数最少是两个，
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.sparkContext.setCheckpointDir("newCp")

    val ris: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    // TODO 窗口
    val wordDS: DStream[String] = ris.flatMap(_.split(" "))
    val wordToOneDS: DStream[(String, Int)] = wordDS.map((_,1))
    // TODO 将多个采集周期作为计算的整体
    // 窗口的范围应该是采集周期的整数倍
    // 默认滑动的幅度（步长）为一个采集周期
    // 窗口的计算得后期等同于窗口的滑动的步长
    // 窗口的范围的大小和滑动的步长应该都是采集周期的整数倍，否则会报错
//    val windowDS: DStream[(String, Int)] = wordToOneDS.window(Seconds(9))
    val windowDS: DStream[(String, Int)] = wordToOneDS.window(Seconds(9), Seconds(5))
    val result: DStream[(String, Int)] = windowDS.reduceByKey(_+_)
    result.print()



    ssc.start()

    // 等待采集器结束
    ssc.awaitTermination()
  }
}
