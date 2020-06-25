package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.Partitioner
/**
  * @Author: lenovo
  * @Time: 2020/6/6 7:53
  * @Description:
  * @Modified By: lenovo
  */
object Spark35_RDD_Operator1717 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // TODO 自定义分区器 - 自己决定数据放置在哪个分区做处理
    // cba, wnba, nba
    val rdd = sc.makeRDD(
      List(
        ("cba", "消息1"),("cba", "消息2"),("cba", "消息3"),
        ("nba", "消息4"),("wnba", "消息5"),("nba", "消息6")
      ),
      1
    )
    val rdd2 = rdd.partitionBy(new MyPartitioner(2))
    println(rdd2.collect().mkString(","))
    sc.stop()
  }
  class MyPartitioner(num:Int) extends Partitioner{
    override def numPartitions: Int = {
      num
    }

    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case _ => 1
      }

    }
  }
}
