package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * @Author: lenovo
  * @Time: 2020/6/22 8:41
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming11_output {
  def main(args: Array[String]): Unit = {
    // SparkStreaming使用核数最少是两个，
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.sparkContext.setCheckpointDir("newCp")

    val ris: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    // 窗口
    val wordToOneDS = ris.map(num => ("key",num.toInt))
    // todo reduceByKeyAndWindow 方法一般用于重复数据的范围比较大的场合，这样可以优化效率
    val result: DStream[(String, Int)] = wordToOneDS.reduceByKeyAndWindow(
      // 计算前移导致的增加
      (x, y) => {
        println(s"x = ${x}, y = ${y}")
        x + y
      },
      // 计算前移导致的减少
      (a, b) => {
        println(s"a = ${a}, b = ${b}")
        a - b
      },
      Seconds(9)
    )

    // result.print()
    // result.saveAsObjectFiles()
    // transform : rdd => rdd
    // foreacheRDD : rdd => Unit

    // rdd.map : A=>B
    // rdd.foreach: A=> Unit
    // Code Driver(1)
    result.foreachRDD(rdd =>
      // Code Driver(N)
      rdd.foreach(
          data =>
          // Code Executor(N)
            println(data)
      )
    )



    ssc.start()

    // 等待采集器结束
    ssc.awaitTermination()
  }
}
