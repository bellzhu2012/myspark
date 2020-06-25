package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 20:32
  * @Description:
  * @Modified By: lenovo
  */
object Spark50_serialize {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4), 2
    )
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    rdd.map((_,1)).saveAsSequenceFile("output2")

    sc.stop()
  }
}
