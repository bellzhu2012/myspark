package com.atguigu.spark.core
import org.apache.spark.Partitioner
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/5 14:25
  * @Description:
  * @Modified By: lenovo
  */
object Spark35_RDD_Opeartor17 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val context = new SparkContext(conf)
    val rdd = context.makeRDD(List(
      ("cba", "消息1"), ("wcba", "消息2"), ("cba", "消息3"),
      ("nba", "消息4"), ("wnba", "消息5"), ("nba", "消息6")
    ))
    val rdd1 = rdd.partitionBy(new MyPartitioner(2))
    val rdd2 = rdd1.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(data => (data, index))
      }
    )
    println(rdd2.collect().mkString(","))
    context.stop()
  }

  // 自定义分区器
  // 继承Partitioner，重写方法
  class MyPartitioner(num : Int) extends Partitioner{
    override def numPartitions: Int = {
      num
    }
    //
    override def getPartition(key: Any): Int = {
      key match{
        case "nba" => 0
        case _ => 1
      }
    }
  }
}
