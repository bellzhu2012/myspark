package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/3 8:01
  * @Description:
  * @Modified By: lenovo
  */
object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val context: SparkContext = new  SparkContext(conf)
    // 2 从磁盘（文件）中创建RDD
    // path：读取文件的路径
    // 可以是设定相对路径，对IDEA而言，相对路径就是project的根路径
    // path相对路径会根据环境变化自动发生更改

    // spark读取文件，默认采取和Hadoop读取文件的规则，一行一行读取
    val rdd: RDD[String] = context.textFile("input")
    println(rdd.collect().mkString(","))
    // path可以使用通配符
    val rdd1: RDD[String] = context.textFile("input/wordcount*.txt")
    println(rdd1.collect().mkString(","))
    // path还可以指向第三方存储系统：HDFS
    context.stop()
    //
  }
}
