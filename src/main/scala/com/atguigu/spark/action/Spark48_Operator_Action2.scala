package com.atguigu.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 22:15
  * @Description:
  * @Modified By: lenovo
  */
object Spark48_Operator_Action2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(2,1,4,3))
    // 先排序，然后取值
    val array: Array[Int] = rdd.takeOrdered(3)
    println(array.mkString(","))
    sc.stop()
  }
}
