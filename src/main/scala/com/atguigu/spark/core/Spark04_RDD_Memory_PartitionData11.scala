package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/3 22:03
  * @Description:
  * @Modified By: lenovo
  */
object Spark04_RDD_Memory_PartitionData11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("File")
    val context = new SparkContext(conf)
    // n内存中的集合数据按照平均分的方式进行分区处理
//    val rdd = context.makeRDD(List(1,2,3,4),2)
    // 12,34
//    rdd.saveAsTextFile("output")
//    val rdd = context.makeRDD(List(1,2,3,4),4)
//    rdd.saveAsTextFile("output1")
    // saveAsTextFile方法的目标文件如已存在会报错
    // 内存中的数据如果不能平分，会将多余的数据放在最后一个分区
    // 内存中的数据基本上是平均分，如果不能整除，会采用一些基本的算法实现
    //List(1,2,3,4,5) = Array(1,2,3,4,5)
    // length = 5, num = 3
    // (0,1,2)
// => 0 => (0, 1) => 1
// => 1 => (1, 3) => 2,3
// => 2 => (3, 5) => 4,5
// Array.slice => 切分数组 => (from , until)
    val rdd = context.makeRDD(List(1,2,3,4,5),3)
    rdd.saveAsTextFile("output2")

    context.stop()
  }
}
