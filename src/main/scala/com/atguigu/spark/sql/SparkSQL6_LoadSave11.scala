package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: lenovo
  * @Time: 2020/6/13 7:38
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL6_LoadSave11 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 导入隐式转换，这里的spark是环境对象
    // 要求这个对象必须使用val声明
    import spark.implicits._
    // SparkSQL通用的读取和保存方式
    val frame = spark.read.load("input/user.parquet")
    // 保存
//    frame.write.save("output")

    frame.write.mode("append").format("json").save("output")
    spark.stop()
  }
}
