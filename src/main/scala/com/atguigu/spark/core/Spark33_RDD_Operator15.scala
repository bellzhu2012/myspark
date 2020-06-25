package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 11:19
  * @Description:
  * @Modified By: lenovo
  */
object Spark33_RDD_Operator15 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val context = new SparkContext(conf)
    val rdd1 = context.makeRDD(List(1,2,3,4),2)
    val rdd2 = context.makeRDD(List(3,4,5,6),2)
    val rdd3: RDD[Int] = rdd1.union(rdd2)
    println(rdd3.collect().mkString(","))
    rdd3.saveAsTextFile("output2")
    val rdd4: RDD[Int] = rdd1.intersection(rdd2)
    rdd4.saveAsTextFile("output3")
    println(rdd4.collect().mkString(","))
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))
    rdd5.saveAsTextFile("output4")
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))
    rdd6.saveAsTextFile("output5")

    context.stop()
  }
}
