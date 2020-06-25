package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/4 10:21
  * @Description:
  * @Modified By: lenovo
  */
object Spark20_RDD_Operator77 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // groupBy方法可以按照指定的规则进行分组，指定规则的返回值就是分组的key
    // groupBy方法执行完毕后，会将数据进行分组操作，但是分区是不会改变的
    // 不同组的数据会打乱在不同的分区中
    // groupBy方法会导致数据不均匀，产生shuffle操作。如果想改变分区，可以传递参数
    // 分组结果决定了数据集中放置在多少个分区中
    val rdd = sc.makeRDD(List(1,2,3,4,5,6),3)
//    val rdd1 = rdd.groupBy(a => a%2 )
    val rdd1 = rdd.groupBy((a:Int) => {a%2},2)
    rdd1.saveAsTextFile("output5")
    sc.stop()
  }
}
