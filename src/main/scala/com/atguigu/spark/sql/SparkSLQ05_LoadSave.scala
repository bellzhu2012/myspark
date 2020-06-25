package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author: lenovo
  * @Time: 2020/6/12 19:41
  * @Description:
  * @Modified By: lenovo
  */
object SparkSLQ05_LoadSave {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 导入隐式转换，这里的spark是环境对象
    // 要求这个对象必须使用val声明
    import spark.implicits._
    // SparkSQL通用的读取和保存方式
    // TODO 读取
    // SparkSQL默认的读取和保存数据格式都是Parquet列式存储格式
//    val frame: DataFrame = spark.read.load("input/user.parquet")
//    frame.show()
    // 如果想要改变读取文件的格式，需要使用特殊的操作
    // TODO 如果读取的文件格式为JSON格式，Spark对JSON文件的格式有要求
    // JavaScript Object Notation
    // Json文件的格式要求整个文件满足JSON的语法
    // Spark读取文件默认是以行为单位来读取
    // Spark读取JSON文件时，要求文件的每一行符合JSON格式
    // 如果文件格式不正确，那么不会发生错误，但是会发生解析结果不正确
    // 总要求是JSON的每一行要满足Spakr的要求，即每一行都是JSON格式，但是整体格式不一定满足JSON文件
    val frame = spark.read.format("json").load("input/user.json")
    frame.show()
    spark.stop()
  }
}
