package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 14:57
  * @Description:
  * @Modified By: lenovo
  */
object Spark37_RDD_Operator20 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val context = new SparkContext(conf)
    val rdd = context.makeRDD(List(("a",1),("a",2),("b",2),("b",1),("c",3)))
    val rdd1 = rdd.groupByKey()
    println(rdd1.collect().mkString(","))
    context.stop()
  }
}
