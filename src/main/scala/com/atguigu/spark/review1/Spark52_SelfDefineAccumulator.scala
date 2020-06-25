package com.atguigu.spark.review1

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @Author: lenovo
  * @Time: 2020/6/8 14:33
  * @Description:
  * @Modified By: lenovo
  */
object Spark52_SelfDefineAccumulator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List("hello scala","spark","hello hive"))
    // 创建累加器
    val accumulator = new MyAccumulator
    // 注册累加器
    sc.register(accumulator)
    // 使用累加器
    rdd.flatMap(_.split(" ")).foreach(
      word =>{
        accumulator.add(word)
      }
    )
    println(accumulator.value)
    // 获取累加器

    sc.stop()
  }
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String,Int]]{

    // 存储wordcount的集合
    var wordCountMap = mutable.Map[String, Int]()

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new MyAccumulator
    }
    // 初始化是否为空
    override def isZero: Boolean ={
      wordCountMap.isEmpty
    }
    // 重置
    override def reset(): Unit = {
      wordCountMap.clear()
    }

    override def add(word: String): Unit = {
      wordCountMap.update(word, wordCountMap.getOrElse(word,0) + 1)
    }

    // 合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      val map1 = wordCountMap
      val map2 = other.value
      wordCountMap = map1.foldLeft(map2)(
        (map,kv) =>{
          map(kv._1) = map.getOrElse(kv._1,0) + kv._2
          map
        }
      )
    }

    // 返回累加器的值
    override def value: mutable.Map[String, Int] = {
      wordCountMap
    }
  }
}
