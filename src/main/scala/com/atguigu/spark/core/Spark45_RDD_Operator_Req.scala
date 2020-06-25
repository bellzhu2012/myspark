package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 10:39
  * @Description:
  * @Modified By: lenovo
  */
object Spark45_RDD_Operator_Req {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 1获取原始数据
    val rdd = sc.textFile("input/agent.log")
    // 2将原始数据进行结构转换 =>（省份-广告，1）
    val rdd1 : RDD[(String,Int)] = rdd.map(
      line => {
        val word = line.split(" ")
        (word(1) + "-" + word(4), 1)
      }
    )
    // 3 将相同的key数据进行分组聚合 => （省份-广告，sum）
    val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_ + _ )
    // 4 将聚合后的结果进行结构的转换 =>（省份，（广告，sum））
    val rdd3: RDD[(String, (String, Int))] = rdd2.map {
      case (key, sum) => {
        val keys = key.split("-")
        (keys(0), (keys(1), sum))
      }
    }
    // 5 将相同省份的结果进行分组 => （省份，Iterator（（广告1，sum1），（广告2，sum2）））
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()
    // 6 将数据进行排序（降序），取Top3
    val rdd5 = rdd4.mapValues(iter => {
      iter.toList.sortWith(
        (left, right) =>
          left._2 > right._2
      ).take(3)
    })
    // 7 控制台d打印
    val result = rdd5.collect()
    result.foreach(println)
    sc.stop()
  }
}
