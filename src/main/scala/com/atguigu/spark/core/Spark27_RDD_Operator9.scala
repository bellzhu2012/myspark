package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 21:57
  * @Description:
  * @Modified By: lenovo
  */
object Spark27_RDD_Operator9 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4,5,6))
    // sample 用于从数据集中抽取数据
    // 第一个参数表示数据抽取后是否放回，可以重复抽取
    // true：收取后放回
    // false：抽取后不放回
    // 第二个参数表示数据抽取的几率（不放回场合），或重复抽取的次数（放回的场合）
    // 该几率不是数据能够抽中的数据总量的比率
    // 第三个参数表示随机数的种子，可以确定数据的抽取
    // 随机数不随机，所谓的随机数依赖随机算法实现
//    val rdd1 = rdd.sample(false,0.5,1)
    val rdd1 = rdd.sample(true,2)
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
