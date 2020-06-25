package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: lenovo
  * @Time: 2020/6/12 14:25
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL_load {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark = SparkSession.builder().config(conf).getOrCreate()
//    val frame = spark.read.format("json").load("input/user.json")
    val frame = spark.read.format("json").load("input/user.json")
    frame.show()
    frame.write.save("output")
    spark.stop()
  }
}
