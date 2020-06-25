package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @Author: lenovo
  * @Time: 2020/6/12 19:56
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL06_LoadSave1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 导入隐式转换，这里的spark是环境对象
    // 要求这个对象必须使用val声明
    import spark.implicits._
    // SparkSQL通用的读取和保存方式
    // TODO 读取
    // SparkSQL默认的读取和保存数据格式都是Parquet列式存储格式
    val frame: DataFrame = spark.read.format("json").load("input/user.json")
    frame.write.mode("ignore").format("json").save("output")
//    case "overwrite" => SaveMode.Overwrite  覆盖
//    case "append" => SaveMode.Append 追加新文件
//    case "ignore" => SaveMode.Ignore 忽略,如果有则没有影响，没有就生成
    spark.stop()
  }
}
