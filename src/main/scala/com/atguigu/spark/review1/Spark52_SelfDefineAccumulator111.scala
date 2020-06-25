package com.atguigu.spark.review1

import com.sun.xml.internal.fastinfoset.algorithm.BuiltInEncodingAlgorithm.WordListener
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @Author: lenovo
  * @Time: 2020/6/8 20:56
  * @Description:
  * @Modified By: lenovo
  */
object Spark52_SelfDefineAccumulator111 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("accumulator")
    val context = new SparkContext(conf)
    val rdd = context.makeRDD(List(
      "hello spark", "hello scala"
    ))
    val acc = new MyAccumulator
    context.register(acc)
    rdd.flatMap(_.split(" ")).foreach(
      word => {
        acc.add(word)
      }
    )
    println(acc.value.mkString(","))
    context.stop()
  }
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String,Int]]{
    private var wordCountMap = mutable.Map[String, Int]()
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
        wordCountMap.update(word,wordCountMap.getOrElse(word,0) + 1)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
        val map1 = wordCountMap
        val map2 = other.value
        wordCountMap = map1.foldLeft(map2)(
          (map, kv) => {
            map.update(kv._1,map.getOrElse(kv._1,0) + kv._2)
            map
          }
        )
    }

    override def value: mutable.Map[String, Int] = {
        wordCountMap
    }
  }
}
