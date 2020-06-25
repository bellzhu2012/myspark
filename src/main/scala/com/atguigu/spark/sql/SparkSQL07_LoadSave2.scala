package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: lenovo
  * @Time: 2020/6/12 20:01
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL07_LoadSave2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 导入隐式转换，这里的spark是环境对象
    // 要求这个对象必须使用val声明
    import spark.implicits._
//    spark.sql("select * from json.`input/user.json").show()
    // 可以通过上下文环境，直接使用spark.sql的方式查询文件，要确定文件格式
    spark.sql("select * from json.`input/user.json`").show()
    spark.stop()
  }
}
