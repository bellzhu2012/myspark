package com.atguigu.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 22:09
  * @Description:
  * @Modified By: lenovo
  */
object Spark47_Operator_Action1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    // 简化规约
    val reduceRDD: Int = rdd.reduce(_ + _)
    // 采集数据
//    collect方法会将所有分区计算的结果拉取到当前节点Driver的内存中
    val coll: Array[Int] = rdd.collect()
    // 统计个数
    val count: Long = rdd.count()
    // first
    val first: Int = rdd.first()
    // take
    val array: Array[Int] = rdd.take(2)
    println(reduceRDD)
    println(coll.mkString(","))
    println(count)
    println(first)
    println(array.mkString(","))
    sc.stop()
  }
}
