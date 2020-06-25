package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 13:19
  * @Description:
  * @Modified By: lenovo
  */
object Spark05_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // textFile 第一个参数表示读取文件的路径
    // textFile 第二个参数表示最小分区数量
    //          默认值为： math.min(defaultParallelism, 2)
    //                     math.min(8, 2) => 2
    val rdd = sc.textFile("input/wordcount.txt")
//    println(rdd.collect().mkString(","))
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
