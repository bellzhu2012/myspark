package com.atguigu.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 20:41
  * @Description:
  * @Modified By: lenovo
  */
object WordcountTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(
      "hello", "scala", "hello", "spark"
    ))
    // 1 countByValue
//    val rdd1: collection.Map[String, Long] = rdd.countByValue()

    // 2 countByKey
//    val rdd1 = rdd.map(word=>(word,1)).countByKey()

    // 3 groupBy
//    val rdd1 = rdd
//      .groupBy(key => key)
//      .map(tuple => (tuple._1, tuple._2.size))

    // 4 reduceBykey
//    val rdd1 = rdd.map((_,1)).reduceByKey(_ + _)
    // 5 groupByKey
//    val map = rdd.map((_,1)).groupByKey()
//    val rdd1 = map.map(
//      tuple => (tuple._1, tuple._2.size)
//    )
    // 6 aggregateByKey
//    val rdd1 = rdd.map((_,1)).aggregateByKey(0)(_ + _, _ + _)
    // 7 foldByKey
//    val rdd1 = rdd.map((_,1)).foldByKey(0)(_+_)
    // 8 combineByKey
//    val rdd1 = rdd.map((_, 1)).combineByKey(
//      v => v,
//      (t: Int, value) => t + value,
//      (t1: Int, t2: Int) => t1 + t2
//    )
    // 9 reduce

val mapRDD: RDD[Map[String, Int]] = rdd.map(word => Map[String,Int]((word, 1)))


    val rdd1 = mapRDD.fold(Map[String, Int]()) {
      case (map1, map2) => {
        map2.foldLeft(map1)(
          (map, kv) => {
            val key = kv._1
            val value = kv._2
            map.updated( key, map.getOrElse(key, 0) + value )
          }
        )
      }
    }
//            val rdd1 = mapRDD.reduce(
//                ( map1, map2 ) => {
//                    map1.foldLeft(map2)(
//                        ( map, kv ) => {
//                            val word = kv._1
//                            val count = kv._2
//                            map.updated( word, map.getOrElse(word, 0) + count )
//                        }
//                    )
//                }
//            )

    println(rdd1.mkString(","))
    sc.stop()
  }
}
