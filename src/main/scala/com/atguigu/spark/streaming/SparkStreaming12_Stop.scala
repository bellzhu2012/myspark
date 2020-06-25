package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
  * @Author: lenovo
  * @Time: 2020/6/22 10:21
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming12_Stop {
  def main(args: Array[String]): Unit = {
    // SparkStreaming使用核数最少是两个，
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    val ris: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordToOneDS = ris.map(num => ("key",num))
    wordToOneDS.print()
    ssc.start()
    // TODO 当业务升级的场合，或逻辑发生变化
    // TODO stop方法一般不会放置在main线程中完成
    // TODO 需要将stop方法使用新的线程完成调用
    new Thread(
      new Runnable {
        override def run(): Unit = {
          // TODO stop方法的调用不应该是线程启动后马上调用
          // TODO stop方法调用的时机，这个时机不容易确定，需要周期性的判断
          while(true){
            Thread.sleep(10000)
            // TODO 关闭时机的判断一般不会使用业务操作
            // TODO 一般采用第三方的程序或者存储来进行判断
            // HDFS
            // ZK
            // MYSQL
            // REDIS
            // 当收到关闭的通知时，优雅的关闭
            val state: StreamingContextState = ssc.getState()
            if (state == StreamingContextState.ACTIVE) {
              // 优雅的关闭，类似黑名单，在当前周期性操作结束后，在关闭整个系统
              ssc.stop(true, true)
              System.exit(0)
            }
          }
        }
      }
    ).start()


    ssc.awaitTermination()
  }
}
