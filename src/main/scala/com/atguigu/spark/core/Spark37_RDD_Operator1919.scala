package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 8:01
  * @Description:
  * @Modified By: lenovo
  */
object Spark37_RDD_Operator1919 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // reduceByKey: 根据数据的key进行分组，然后对value进行聚合
    val rdd = sc.makeRDD(List(("a",1),("b",2),("a",4),("b",3)))
    // reduceByKey参数说明
    // （1）表示相同的key的value的聚合方式
    // （2）表示聚合后的分区数量
    val rdd1 = rdd.reduceByKey(_ + _)
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
