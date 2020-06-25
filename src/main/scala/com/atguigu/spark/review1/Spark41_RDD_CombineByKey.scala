package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 19:44
  * @Description:
  * @Modified By: lenovo
  */
object Spark41_RDD_CombineByKey {

    def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
      val sc = new SparkContext(sparkConf)
      // TODO combineByKey方法可以传递3个参数
      //  第一个参数表示的就是将计算的第一个值转换结构
      //  第二个参数表示分区内的计算规则
      //  第三个参数表示分区间的计算规则
      val rdd = sc.makeRDD(List(
        ("a", 88), ("b", 95), ("a", 91),
        ("b", 93), ("a", 95), ("b", 98)
      ), 2)
      val rdd1 = rdd.combineByKey(
        v => (v, 1),
        (t: (Int, Int), v) => {
          (t._1 + v, t._2 + 1)
        },
        (a: (Int, Int), b: (Int, Int)) => {
          (a._1 + b._1, a._2 + b._2)
        }
      )
      val rdd2 = rdd1.map {
        case (key, (total, count)) => {
          (key, total / count)
        }
      }
      println(rdd2.collect().mkString(","))
      sc.stop()
    }

}
