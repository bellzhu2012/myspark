package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import scala.collection.mutable

/**
  * @Author: lenovo
  * @Time: 2020/6/22 12:31
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming03_Queue111 {
  def main(args: Array[String]): Unit = {
    // SparkStreaming使用核数最少是两个，
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 通过队列创建DS
    val queue = new mutable.Queue[RDD[String]]()
    val queueDS = ssc.queueStream(queue)
    queueDS.print()

    ssc.start()
    for( i <- 1 to 5){
        val rdd = ssc.sparkContext.makeRDD(List(i.toString))
        queue.enqueue(rdd)
        Thread.sleep(1000)
    }
    ssc.awaitTermination()
  }
}
