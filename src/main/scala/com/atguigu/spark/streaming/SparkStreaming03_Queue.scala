package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @Author: lenovo
  * @Time: 2020/6/15 20:45
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming03_Queue {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    // 从内存队列中获取DStream
    val queue: mutable.Queue[RDD[String]] = new mutable.Queue[RDD[String]]()
    val queueDS: InputDStream[String] = ssc.queueStream(queue)
    queueDS.print()

    ssc.start()

    for( i <- 1 to 5){
      val rdd: RDD[String] = ssc.sparkContext.makeRDD(List(i.toString()))
        queue.enqueue(rdd)
        Thread.sleep(1000)
    }
    ssc.awaitTermination()
  }
}
