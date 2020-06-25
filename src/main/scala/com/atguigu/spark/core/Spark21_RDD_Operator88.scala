package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 10:37
  * @Description:
  * @Modified By: lenovo
  */
object Spark21_RDD_Operator88 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 过滤
    // 根据指定的规则对数据进行筛选，满足条件的数据保留，不满足的就丢弃
    // 可能会导致数据倾斜
    val rdd = sc.makeRDD(List(1,2,3,4,5,6),3)
    val rdd1 = rdd.filter(_ % 2 ==0)
    rdd1.collect().foreach(println)
    sc.stop()
  }
}
