package com.atguigu.spark.streaming

import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import java.net.Socket

import scala.collection.mutable

/**
  * @Author: lenovo
  * @Time: 2020/6/22 12:40
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming05_DIY11 {
  def main(args: Array[String]): Unit = {
    // SparkStreaming使用核数最少是两个，
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 通过自定义接收器创建DS
    val ds = ssc.receiverStream(new MyReceiver("localhost",9999))

    ds.print()

    ssc.start()

    ssc.awaitTermination()
  }
  class MyReceiver(host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

    private var socket : Socket = _
    def receive() : Unit = {
      val reader = new BufferedReader(
        new InputStreamReader(
          socket.getInputStream,
          "UTF-8"
        )
      )
      var s : String = null
      while(true){

        s = reader.readLine()
        if ( s != null) {
          store(s)
        }
      }
    }

    override def onStart(): Unit = {
      socket = new Socket(host, port)
      new Thread("Socket Receiver"){
        setDaemon(true)
        override def run(): Unit ={
          receive()
        }
      }.start()
    }

    override def onStop(): Unit = {
        socket.close()
        socket = null
    }
  }
}
