package com.atguigu.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 22:33
  * @Description:
  * @Modified By: lenovo
  */
object Spark51_Operator_Action5 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4))
    // 集合方法中的代码是在当前节点Driver中执行
    // foreach方法是在当前节点的内存中完成数据的循环
    rdd.collect().foreach(println)
    // rdd的方法称为算子
    // 算子的逻辑代码是在分布式计算节点的executor上执行的
    // foreach算子可以将循环在不同的计算节点完成
    // 算子之外的逻辑都是在Driver端执行的
    rdd.foreach(println)
    sc.stop()
  }
}
