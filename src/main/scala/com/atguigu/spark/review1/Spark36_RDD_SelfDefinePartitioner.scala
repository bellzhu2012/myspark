package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.Partitioner
/**
  * @Author: lenovo
  * @Time: 2020/6/7 18:36
  * @Description:
  * @Modified By: lenovo
  */
object Spark36_RDD_SelfDefinePartitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val rdd1 = rdd.partitionBy(new MyPartitioner(2))
    rdd1.saveAsTextFile("output1")
    sc.stop()
  }
  class MyPartitioner(var num:Int) extends Partitioner{
    override def numPartitions: Int = {
      num
    }

    override def getPartition(key: Any): Int = {
      key.hashCode() % num
    }
  }
}
