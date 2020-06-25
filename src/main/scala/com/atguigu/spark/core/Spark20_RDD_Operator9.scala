package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/3 17:05
  * @Description:
  * @Modified By: lenovo
  */
object Spark20_RDD_Operator9 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val context: SparkContext = new SparkContext(conf)
    //    val rdd = context.makeRDD(List(List(1,2),List(3,4)),2)
    val rdd = context.makeRDD(List(1,2,3,4,5,6),3)
    // 旧RDD->算子->新RDD

    val rdd1 = rdd.groupBy((num:Int)=>{num % 2 },2)
    rdd1.collect().foreach(println)

    context.stop()
  }
}
