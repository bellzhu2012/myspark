package com.atguigu.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 11:04
  * @Description:
  * @Modified By: lenovo
  */
object Spark55_Dep1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    //  new ParallelCollectionRDD[0]
    // List()
    val rdd: RDD[String] = sc.makeRDD(List(
      "hello scala", "hello spark"
    ))
    println(rdd.dependencies)
    println("-"*30)
    // new MapPartitionsRDD[1] -> new ParallelCollectionRDD[0]
    // OneToOneDependency
    val rdd1 = rdd.flatMap(
      string => string.split(" ")
    )
    println(rdd1.dependencies)
    println("-"*30)
    // new MapPartitionsRDD[2] -> new MapPartitionsRDD[1] -> new ParallelCollectionRDD[0]
    // OneToOneDependency
    val rdd2 = rdd1.map(
      word => (word, 1)
    )
    println(rdd2.dependencies)
    println("-"*30)
    // new ShuffledRDD[3] ->new MapPartitionsRDD[2] -> new MapPartitionsRDD[1] -> new ParallelCollectionRDD[0]
    // ShuffleDependency
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    println(rdd3.dependencies)
    println("-"*30)
    // 如果Spark的计算过程某一个节点计算失败，那么框架会尝试重新计算
    // Spark既然想要重新计算，那么就需要知道数据的来源，而且经历了哪些计算
    // rdd不保存计算数据，但是会保存元数据的信息

    sc.stop()
  }
}
