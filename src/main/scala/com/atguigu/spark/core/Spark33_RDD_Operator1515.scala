package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 7:38
  * @Description:
  * @Modified By: lenovo
  */
object Spark33_RDD_Operator1515 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 双value：双RDD操作
    var rdd1 = sc.makeRDD(List(1,2,3,4),2)
    var rdd2 = sc.makeRDD(List(3,4,5,6),2)
    // 并集
    var rdd3: RDD[Int] = rdd1.union(rdd2)
    println(rdd3.collect().mkString(","))
    // 交集
    val rdd4: RDD[Int] = rdd1.intersection(rdd2)
    println(rdd4.collect().mkString(","))

    // 差集
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    // 拉链
    // 拉链要求两个rdd的分区数和分数内数据量都一致，否则就报错
    // 对泛型没有要求
    val rdd6 = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))


    sc.stop()
  }
}
