package com.atguigu.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 22:29
  * @Description:
  * @Modified By: lenovo
  */
object Spark50_Operator_Action4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4))
    rdd.saveAsTextFile("output1")
    // saveAsObjectFile和saveAsSequenceFile会对数据进行序列化
    rdd.saveAsObjectFile("output2")
    rdd.map((_,1)).saveAsSequenceFile("output3")
    sc.stop()
  }
}
