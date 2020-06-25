package com.atguigu.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 22:06
  * @Description:
  * @Modified By: lenovo
  */
object Spark46_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    // 所谓的行动算子，不会产生新的RDD，而是触发作业的执行
    // 行动算子执行后，会获取到作业的执行结果
    // 转换算子不会触发作业的执行，只是功能的扩展和包装
    val result: Array[Int] = rdd.collect()
    result.foreach(println)
    sc.stop()
  }
}
