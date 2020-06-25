package com.atguigu.spark.review1

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @Author: lenovo
  * @Time: 2020/6/8 14:59
  * @Description:
  * @Modified By: lenovo
  */
object Spark52_selfDefineAccumulator1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val accumulator = new MyAccumulator
    sc.register(accumulator)
    val rdd = sc.makeRDD(List(
      "hello scala", "hello spark"
    ))
    rdd.flatMap(_.split(" ")).foreach(
      word => accumulator.add(word)
    )
    println(accumulator.value)
    sc.stop()
  }
  class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{
    var wordCountMap = mutable.Map[String,Int]()

    override def isZero: Boolean = {
      wordCountMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new MyAccumulator
    }

    override def reset(): Unit = {
      wordCountMap.clear()
    }

    override def add(word: String): Unit = {
      wordCountMap.update(word, wordCountMap.getOrElse(word, 0) + 1)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      val map1 = wordCountMap
      val map2 = other.value
      wordCountMap = map1.foldLeft(map2)(
        (map,kv) => {
          map.update(kv._1, map.getOrElse(kv._1,0) + kv._2)
          map
        }
      )
    }

    override def value: mutable.Map[String, Int] = {
      wordCountMap
    }
  }
}
