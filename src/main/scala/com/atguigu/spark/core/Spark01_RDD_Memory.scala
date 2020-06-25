package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/3 7:41
  * @Description:
  * @Modified By: lenovo
  */
object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    // Spark--从内存中创建RDD
    // 1 parallelize:并行
    val rdd = sparkContext.parallelize(List(1,2,3,4))
    println(rdd.collect().mkString("-"))
    // 2 使用makeRDD方法
    val rdd2: RDD[Int] = sparkContext.makeRDD(List(1,2,3,4))
    println(rdd2.collect().mkString(":"))
    sparkContext.stop()
  }
}
