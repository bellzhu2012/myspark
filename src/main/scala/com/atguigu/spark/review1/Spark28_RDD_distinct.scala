package com.atguigu.spark.review1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 17:34
  * @Description:
  * @Modified By: lenovo
  */
object Spark28_RDD_distinct {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,1,2,4))
    val rdd1 = rdd.distinct()
    // distinct可以改变分区的数量,底层会调用reduceByKey
    val rdd2 = rdd.distinct(2)
    println(rdd2.toDebugString)
    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))


    sc.stop()
  }
}
