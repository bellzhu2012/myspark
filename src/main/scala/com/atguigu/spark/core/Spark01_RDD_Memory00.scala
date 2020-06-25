package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/3 21:27
  * @Description:
  * @Modified By: lenovo
  */
object Spark01_RDD_Memory00 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("memory")
    val context = new SparkContext(conf)
    // 从内存中创建RDD
    // parallelize：并行
    var list = List(1,2,3,4)
//    val rdd = context.parallelize(list)
    // makeRDD的底层代码其实就是调用了parallelize
    val rdd = context.makeRDD(list)

    println(rdd.collect().mkString(","))
    context.stop()
  }
}
