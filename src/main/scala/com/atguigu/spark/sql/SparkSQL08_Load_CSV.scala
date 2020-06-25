package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: lenovo
  * @Time: 2020/6/12 20:24
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL08_Load_CSV {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 导入隐式转换，这里的spark是环境对象
    // 要求这个对象必须使用val声明
    import spark.implicits._
    val frame = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true") // 是否包含结构信息
      .option("header", "true") // 第一行为字段
      .load("input/user.csv")
    frame.show()

    spark.stop()
  }
}
