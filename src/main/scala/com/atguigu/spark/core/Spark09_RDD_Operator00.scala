package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 9:15
  * @Description:
  * @Modified By: lenovo
  */
object Spark09_RDD_Operator00 {
  def main(args: Array[String]): Unit = {
    // RDD-算子（方法）
    // 转换算子
    // 能够将旧的RDD通过方法转换为新的RDD，但是不会出去作业的执行
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4))
    val rdd1 = rdd.map(_ * 2)
    // 读取数据
    // collect方法不会转换RDD，会触发作业的执行，所以将collect这样的方法称之为行动（action）算子
    val ints = rdd1.collect()
//    rdd1.saveAsTextFile("output4")
    sc.stop()
  }
}
