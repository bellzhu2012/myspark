package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/8 15:42
  * @Description:
  * @Modified By: lenovo
  */
object Spark53_BC {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 广播变量
    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 1), ("c", 1)
    ))
//    val rdd2 = sc.makeRDD(List(
//      ("a", 1), ("b", 1), ("c", 1)
//    ))
    val list = List(("a", 1), ("b", 1), ("c", 1))
    // join会有笛卡尔积的效果 ，数据量如果很大的会，如果有shuffle操作，性能会很低
//    val rdd3 = rdd1.join(rdd2)
    // 使用广播变量
    val bcList = sc.broadcast(list)
    val rdd3 = rdd1.map(
      tuple  => {
        var word = tuple._1
        var count = tuple._2
        var count1 = 0
        for (list <- bcList.value) {
          val key = list._1
          val value = list._2
          if (word == key) {
            count1 = value
          }
        }
        (word, (count, count1))
      }
    )

    println(rdd3.collect().mkString(","))
    sc.stop()
  }
}
