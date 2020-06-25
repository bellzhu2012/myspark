package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @Author: lenovo
  * @Time: 2020/6/22 13:05
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming08_State11 {
  def main(args: Array[String]): Unit = {
    // SparkStreaming使用核数最少是两个，
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.sparkContext.setCheckpointDir("cp")
    val ds = ssc.socketTextStream("localhost", 9999)

    // 数据的有状态保存
    // 将spark每个采集周期数据的处理结果保存起来，然后和后续的数据进行聚合

    // reduceByKey方法是无状态的，而我么需要的有状态的数据操作
    // 有状态的目的其实就是将每一个采集周期的数据计算结果临时保存起来
    // 然后再一次数据的处理可以继续使用
    ds
      .flatMap(_.split(" "))
      .map((_,1L))
      .updateStateByKey[Long](
      (seq:Seq[Long], buffer:Option[Long]) => {
        val newBufferValue = buffer.getOrElse(0L) + seq.sum
        Option(newBufferValue)
      }
    ).print()

    ssc.start()

    ssc.awaitTermination()
  }
}
