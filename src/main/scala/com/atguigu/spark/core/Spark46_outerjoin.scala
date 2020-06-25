package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 13:02
  * @Description:
  * @Modified By: lenovo
  */
object Spark46_outerjoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("File")
    val context = new SparkContext(conf)
    val rdd1 = context.makeRDD(
      List(("a", 1), ("b", 1), ("c", 1),("e", 1))
    )
    val rdd2 = context.makeRDD(
      List(("a", 2), ("b", 2), ("c", 2),("d",1))
    )
    val rdd3 = rdd1.leftOuterJoin(rdd2)
    println(rdd3.collect().mkString(","))
    val rdd4 = rdd1.rightOuterJoin(rdd2)
    println(rdd4.collect().mkString(","))
    context.stop()
  }
}
