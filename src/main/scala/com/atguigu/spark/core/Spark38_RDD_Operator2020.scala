package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 8:07
  * @Description:
  * @Modified By: lenovo
  */
object Spark38_RDD_Operator2020 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // groupByKey: 根据数据的key进行分组
    // groupBy：根据指定的规则进行分组
    // 调用groupByKey后，返回数据的类型为元组
    // 元组的第一个元素表示分组的key
    // 元组的第二个元素表示分组后，相同key的value的集合
    val rdd = sc.makeRDD(
      List(
        ("hello", 4), ("scala",1), ("hello",1),("scala",2)
      )
    )

    val rdd1 = rdd.groupByKey()
    val rdd2 = rdd1.map {
      case (word, iter) => {
        (word, iter.sum)
      }
    }

    println(rdd2.collect().mkString(","))
    sc.stop()
  }
}
