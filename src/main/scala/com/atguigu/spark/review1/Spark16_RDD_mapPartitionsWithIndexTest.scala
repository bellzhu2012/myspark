package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/7 14:24
  * @Description:
  * @Modified By: lenovo
  */
object Spark16_RDD_mapPartitionsWithIndexTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)
    val rdd1 = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.toIterator
        }
      }
    )
    rdd1.collect().foreach(println)
    sc.stop()
  }
}
