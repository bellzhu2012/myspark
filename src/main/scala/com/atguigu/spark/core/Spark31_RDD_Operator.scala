package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 22:19
  * @Description:
  * @Modified By: lenovo
  */
object Spark31_RDD_Operator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,1,1,2,2,2),3)
    val rdd1 = rdd.coalesce(2)
    // 扩大分区，可以使用rePartition方法，代替coalesce
    val rdd2 = rdd1.repartition(6)
    rdd2.saveAsTextFile("output3")
    sc.stop()
  }
}
