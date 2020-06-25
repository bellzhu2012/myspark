package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: lenovo
  * @Time: 2020/6/12 21:25
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL12_Load_Hive {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config(conf).getOrCreate()
    // 导入隐式转换，这里的spark是环境对象
    // 要求这个对象必须使用val声明
    import spark.implicits._
//    spark.sql("create table aa(id int)")
    spark.sql("show databases").show()
    spark.sql("show tables").show()
    spark.stop()
  }
}
