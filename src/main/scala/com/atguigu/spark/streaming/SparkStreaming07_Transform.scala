package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/22 7:39
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming07_Transform {
  def main(args: Array[String]): Unit = {
    // SparkStreaming使用核数最少是两个，
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    val ris: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    // TODO 转换
    // Code Driver(1)
    val newDS: DStream[String] = ris.transform(
      rdd =>
        // Code Driver(N)
        rdd.map(
          data =>
            // Code Executor(N)
            data * 2
        )
    )
    println("......")
    newDS.print()
    println("......")

    // Code:Driver(1)
    val newDS1 = ris.map(
      data =>
        // Code:Executor(N)
        data * 2
    )
    newDS1.print()


    ssc.start()

    // 等待采集器结束
    ssc.awaitTermination()
  }
}
