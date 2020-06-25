package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 10:56
  * @Description:
  * @Modified By: lenovo
  */
object Spark45_RDD_Operator_Req1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 数据初始结构：（时间 省份 城市 用户 广告）
    // 数据处理后的最终结构：（省份1，Iterator(（广告1，次数1），（广告2，次数2）)） 降序
    // 实现步骤：
    //（时间 省份 城市 用户 广告） --> map-->  改变结构
    // (省份-广告，1) -->reduceByKey -->  提前分组聚合（按照省份-广告）
    // （省份-广告，sum）--> map-->  改变结构
    // （省份，（广告1，sum1）） --> 再次按省份分组-->
    // （省份，Iterator(（广告1，sum1）,(广告2，sum2)） -->排序，取前三
    // （省份，Iterator(（广告1，top1）,(广告2，top2)）
    // 读取数据
    val rdd: RDD[String] = sc.textFile("input/agent.log")
    // （时间 省份 城市 用户 广告） --> map-->  （省份-广告，1)
    val rdd2: RDD[(String, Int)] = rdd.map(line => {
      val words: Array[String] = line.split(" ")
      (words(1) + "-" + words(4), 1)
    })
    // (省份-广告，1) -->reduceByKey -->  （省份-广告，sum）
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    // (省份-广告，sum）--> map-->  (省份，（广告1，sum1））
    val rdd4: RDD[(String, (String, Int))] = rdd3.map {
      case (key, count) => {
        val keys = key.split("-")

        (keys(0), (keys(1), count))
      }
    }
    // （省份，（广告1，sum1）） --> 再次按省份分组-->(省份，Iterator(（广告1，sum1）,(广告2，sum2)）)
    val rdd5: RDD[(String, Iterable[(String, Int)])] = rdd4.groupByKey()
    // （省份，Iterator(（广告1，sum1）,(广告2，sum2)） -->（省份，Iterator(（广告1，top1）,(广告2，top2)）
    val result = rdd5.mapValues(
      value => {
        value.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(3)
      }
    )
//    rdd3.collect().foreach(println)
    result.collect().foreach(println)
    sc.stop()
  }
}
