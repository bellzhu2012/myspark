package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/3 22:18
  * @Description:
  * @Modified By: lenovo
  */
object Spark05_RDD_File_Par11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("File")
    val context = new SparkContext(conf)
    // 从磁盘（File）中创建RDD
    // textFile 第一个参数表示读取文件的路径
    // textFile 第二个数据表示最小分区数量
    // 默认值：math.min(defaultParallelism,2)
    //         math.min(8,2) => 2
//    val rdd = context.textFile("input/file.txt")
//    rdd.saveAsTextFile("output")
//    val rdd = context.textFile("input/file.txt",1)
//    rdd.saveAsTextFile("output1")
//    val rdd = context.textFile("input/file.txt",4)
//    rdd.saveAsTextFile("output2")
    val rdd = context.textFile("input/file.txt",3)
    rdd.saveAsTextFile("output3")
    context.stop()
  }
}
