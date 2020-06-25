package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 22:12
  * @Description:
  * @Modified By: lenovo
  */
object Spark30_RDD_Operator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,1,1,2,2,2),2)
    // 扩大分区
    // coalesce的主要目的是减少分区，扩大分区时没有效果
    // 为什么不能扩大，因为在分区缩减时，数据不会打乱从新组合，没有shuffle过程
    // 如果想要将数据扩大分区，就必须打乱数据后重新组合，必须使用shuffle(随机分配)
    // coalesce的第一个参数时缩减分区后的分区数
    // coalesce的第二个参数是表示分区改变是，是否会打乱重新组合，默认是不打乱
    val rdd1 = rdd.coalesce(6,true)
    rdd1.saveAsTextFile("output1")
    sc.stop()
  }
}
