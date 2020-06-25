package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 22:08
  * @Description:
  * @Modified By: lenovo
  */
object Spark29_RDD_Operator11 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,1,1,2,2,2),6)
    val rdd1 = rdd.filter(i => i%2 ==0 )
    // 但数据过滤后，发现数据不够均匀，可以减少分区
    // 削减的分区是随机的，如果有数据，则发生迁移
    val rdd2 = rdd1.coalesce(3)
    rdd2.saveAsTextFile("ooutput")
    sc.stop()
  }
}
