package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @Author: lenovo
  * @Time: 2020/6/22 12:55
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming07_Transform1 {
  def main(args: Array[String]): Unit = {
    // SparkStreaming使用核数最少是两个，
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 通过队列创建DS
    val ds = ssc.socketTextStream("localhost",9999)
    // TODO 转换
    // Code Driver(1)
    val newDS: DStream[String] = ds.transform(
      rdd => {
        // CodeDriver(N)
        rdd.map(
          data => {
            data * 2
          }
        )
      }
    )
    // Code:Driver(1)
    val newDS1 = ds.map(data => {
      // Code:Executor(N)
      data * 2
    })
    newDS1.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
