package com.atguigu.spark.core

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 7:44
  * @Description:
  * @Modified By: lenovo
  */
object Spark34_RDD_Operator1616 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3)),1)
    // 如果数据类型是k-v类型，那么spark会给rdd自动补充很多新的功能
    // 隐式转换
    // partitionBy方法来自于PariRDDFunction类
    // rdd的伴生对象中提供了隐式函数，可以将RDD[K,V]转换为PairRDDFunction类对象
    // partitionBy：按照指定规则进行分区
    // 参数为分区器对象：HashPartitioner & RangePartitioner 或者自定义分区器
    // HashPartitioner是默认分区器，分区规则：对key进行哈希，然后对分区数进行取模操作
    val rdd1 = rdd.partitionBy(new HashPartitioner(2))
    rdd1.saveAsTextFile("output2")
    // sortBy 使用了RangePartitioner（要求key可排序）
    sc.stop()
  }
}
