package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 9:26
  * @Description:
  * @Modified By: lenovo
  */
object Spark10_RDD_Operator11 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 2个分区 => 12,34
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    //分区问题，rdd中分区列表
    // 默认分区数量不变，数据会转换后输出
    val rdd1 = rdd.map(_ * 2)
    rdd1.saveAsTextFile("output3")
    sc.stop()
  }
}
