package com.atguigu.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 22:18
  * @Description:
  * @Modified By: lenovo
  */
object Spark49_Operator_Action3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
//    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    // aggregate: 分区内和分区间都参与运算
//    val sum: Int = rdd.aggregate(10)(_ + _,_ + _)
//    println(sum)
    // fold: 分区间和分区内计算规则相同的aggregate
//    val count: Int = rdd.fold(10)(_+_)
//    println(count)
    // map-->countByKey:只计算相同key的数量
//    val rdd = sc.makeRDD(
//      List(
//        ("a", 2), ("b", 2), ("a", 1)
//      ))
//    val rdd1: collection.Map[String, Long] = rdd.countByKey()
//    println(rdd1.mkString(","))
    // countByValue: 对相同值的数据进行分组聚合
    val rdd = sc.makeRDD(
      List("a", "b", "a", "c", "c", "a")
    )
    val map: collection.Map[String, Long] = rdd.countByValue()
    println(map.mkString(","))
    sc.stop()
  }
}
