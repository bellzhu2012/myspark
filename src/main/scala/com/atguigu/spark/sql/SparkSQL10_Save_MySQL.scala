package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @Author: lenovo
  * @Time: 2020/6/12 20:50
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL10_Save_MySQL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 导入隐式转换，这里的spark是环境对象
    // 要求这个对象必须使用val声明
    import spark.implicits._
    val frame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver") // 是否包含结构信息
      .option("user", "root") // 第一行为字段
      .option("password", "123456") // 第一行为字段
      .option("dbtable", "customers") // 第一行为字段
      .load("input/user.csv")
    frame.show()
    // 向mysql中创建一张表
    frame.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver") // 是否包含结构信息
      .option("user", "root") // 第一行为字段
      .option("password", "123456") // 第一行为字段
      .option("dbtable", "user") // 第一行为字段
      .mode(SaveMode.Append)
      .save()
    // DataFrame会根据读取表格的结构，创建新的表
    spark.stop()
  }
}
