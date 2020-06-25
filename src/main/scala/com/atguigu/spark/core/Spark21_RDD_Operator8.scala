package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/3 21:16
  * @Description:
  * @Modified By: lenovo
  */
object Spark21_RDD_Operator8 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context: SparkContext = new SparkContext(conf)
    // 将List(List(1,2),3,List(4,5))进行扁平化处理
    val dataRDD: RDD[Int] = context.makeRDD(List(1,2,3,4,5,6),3)
    // 过滤
    // 根据指定的规则将数据进行筛选过滤，满足条件的数据保留，不满足的数据丢弃
    // 可能会导致数据倾斜
    val rdd = dataRDD.filter((data:Int) => {data % 2 == 0})
    rdd.collect().foreach(println)
    context.stop()
  }
}
