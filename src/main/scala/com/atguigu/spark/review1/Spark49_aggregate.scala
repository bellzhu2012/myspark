package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 20:24
  * @Description:
  * @Modified By: lenovo
  */
object Spark49_aggregate {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(
      "a", "a", "b", "hello", "hello"
    ))
    // aggregate:初始值会同时参与到分区内和分区间计算
//    val rdd = sc.makeRDD(List(1,2,3,4),2)
//    val rdd1 = rdd.aggregate(10)(
//      (x, y) => x + y,
//      (x, y) => x + y
//    )
//    println(rdd1)
    // countByKey: 按照key分组，value值为key出现的次数
//    val rdd = sc.makeRDD(List(
//      ("a", 4), ("a", 4), ("a", 4)
//    ))
//    val collect: collection.Map[String, Long] = rdd.countByKey()
//    println(collect)
    val map: collection.Map[String, Long] = rdd.countByValue()
    println(map)
    sc.stop()
  }
}
