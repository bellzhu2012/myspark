package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 8:26
  * @Description:
  * @Modified By: lenovo
  */
object Spark41_RDD_Operator2323 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)

    // TODO combineByKey

    // TODO 每个key的平均值 : 相同key的总和 / 相同key的数量

    // 0 =>【("a", 88), ("b", 95), ("a", 91)】
    // 1 =>【("b", 93), ("a", 95), ("b", 98)】
    val rdd = sc.makeRDD(
      List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2
    )
    // 计算时，需要将value的格式发生改变，而且只需要将一个v发生结构改变即可
    // 如果计算时，发现相同的key的value不符合计算规则的话，那么选择combineByKey
    // combineByKey方法可以传递三个参数
    // (1) 第一个参数表示的就是将计算的第一个值进行结构转换
    // (2) 第二个参数表示的是分区内的计算规则
    // (3) 第三个参数表示的是分区间的计算规则
    val rdd1 = rdd.combineByKey(
      v => (v, 1),
      (t:(Int, Int), v) => (t._1 + v , t._2 + 1),
      (t1:(Int,Int),t2:(Int,Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    val rdd2 = rdd1.map {
      case (key, (total, count)) => {
        (key, total / count)
      }
    }
    println(rdd2.collect().mkString(","))
  }
}
