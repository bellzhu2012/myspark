package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * @Author: lenovo
  * @Time: 2020/6/22 7:58
  * @Description:
  * @Modified By: lenovo
  */
object SparkStreaming08_State {
  def main(args: Array[String]): Unit = {
    // SparkStreaming使用核数最少是两个，
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.sparkContext.setCheckpointDir("newCp")

    val ris: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    // 数据的有状态的保存
    // 将spark每个采集周期的数据处理结果保存起来，然后和后续的数据进行聚合
    // reduceByKey是无状态的，而我们需要的是有状态的数据操作

    // 所谓有状态的目的其实就是将每一个采集周期数据的计算结果临时保存起来
    // 然后在下一次的数据处理中可以继续使用
    ris.flatMap(_.split(" "))
      .map((_,1L))
//      .reduceByKey(_+_)
      // updateStateByKey是有状态计算方法
      // 第一个参数表示 相同key的value的集合
      // 第二个参数表示 想用key的缓冲区的数据，有可能为空
      // requirement failed: The checkpoint directory has not been set.
      // 这里的计算的中间结果需要保存到检查点的位置中,所以需要设定检查点路径
        .updateStateByKey(
      (seq:Seq[Long], buffer:Option[Long])=>{
        val newBufferValue = buffer.getOrElse(0L) + seq.sum
        Option(newBufferValue)
      }
    )

      .print()



    ssc.start()

    // 等待采集器结束
    ssc.awaitTermination()
  }
}
