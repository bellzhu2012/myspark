package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: lenovo
  * @Time: 2020/6/13 7:48
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL09_Load_MySQL11 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 导入隐式转换，这里的spark是环境对象
    // 要求这个对象必须使用val声明
    import spark.implicits._
    // SparkSQL通用的读取和保存方式
    spark.read.format("jdbc")
        .option("url","jdbc:mysql://localhost:3306/test")
        .option("driver","com.mysql.jdbc.Driver")
        .option("user","root")
        .option("password","123456")
        .option("dbtable","customers")
        .load().show()
    spark.stop()
  }
}
