package com.atguigu.spark.review1

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 18:11
  * @Description:
  * @Modified By: lenovo
  */
object Spark35_RDD_partitionBy {
  def main(args: Array[String]): Unit = {
    //key-value类型
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(
      List(("a", 1), ("b", 2), ("c", 2))
    )
    val rdd1 = rdd.partitionBy(new HashPartitioner(2))
    println(rdd1.toDebugString)
    rdd1.saveAsTextFile("output")

    sc.stop()
  }
}
