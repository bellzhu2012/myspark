package com.atguigu.spark.review1

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/8 14:13
  * @Description:
  * @Modified By: lenovo
  */
object Spark51_accumulator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4))
    val sum: LongAccumulator = sc.longAccumulator("sum")
    val result: Unit = rdd.foreach(num => {
      sum.add(num)
    })

    println(sum.value)
    sc.stop()
  }
}
