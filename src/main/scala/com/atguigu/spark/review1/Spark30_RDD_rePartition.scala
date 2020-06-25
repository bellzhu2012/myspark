package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 17:50
  * @Description:
  * @Modified By: lenovo
  */
object Spark30_RDD_rePartition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,1,1,2,2,2),6)
    val rdd1 = rdd.coalesce(2)
    val rdd2 = rdd1.repartition(6)
    rdd2.saveAsTextFile("output")
    sc.stop()
  }
}
