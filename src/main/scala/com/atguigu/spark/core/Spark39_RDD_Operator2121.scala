package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 8:13
  * @Description:
  * @Modified By: lenovo
  */
object Spark39_RDD_Operator2121 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // reduceByKey : 分区内和分区间计算规则相同
    // 如果分区内和分区间计算规则不同 => aggregateByKey
    // TODO 将分区内相同key取最大值，分区间相同的key求和

    // 0 => 【 (a,2), (c,3) 】
    //                         => 【 (a,2)，(b,4)，(c,9) 】
    // 1 => 【 (b,4), (c,6)】
    val rdd = sc.makeRDD(
      List(
        ("a", 1), ("a",2), ("c",3),
        ("b", 4), ("c",5), ("c",6)
      )
      ,2
    )
    // aggregateByKey: 根据key进行数据聚合
    // 函数柯里化
    // 两个参数列表
    // 第一个参数列表（zeroValue）：表示分区内计算的初始值
    // 第二个参数列表：
    //            seqOp：分区内的计算规则，相同的key的value进行聚合
    //            comOp：分区间的计算规则，相同的key的value进行聚合
    val rdd2 = rdd.aggregateByKey(0)(
      (x, y) => {
        math.max(x, y)
      },
      (x, y) => {
        x + y
      }
    )
    println(rdd2.collect().mkString(","))
    sc.stop()
  }
}
