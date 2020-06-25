package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 22:22
  * @Description:
  * @Modified By: lenovo
  */
object Spark32_RDD_Opearator14 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,4,2,3))
    // sortBy 默认为升序
    // sortBy可以通过第二个参数控制排序方式，第三个参数可以改变分区
    val rdd1 = rdd.sortBy(num => num, false,1)
    rdd1.saveAsTextFile("output2")
//    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
