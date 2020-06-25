package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/7 14:13
  * @Description:
  * @Modified By: lenovo
  */
object Spark14_RDD_mapPartitionsTest {
  def main(args: Array[String]): Unit = {
    // TODO Spark - RDD - 算子（方法）
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    //    val rdd1 = rdd.mapPartitions(
    //      iter => {
    //        List(iter.max).toIterator
    //      }
    //    )
        val rdd1: RDD[Array[Int]] = rdd.glom()
    val rdd2: RDD[Int] = rdd1.map(
      i => i.max
    )

    println(rdd2.collect().mkString(","))
  }
}
