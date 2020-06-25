package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: lenovo
  * @Time: 2020/6/13 7:58
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL11_LoadHive {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport()
        .config(conf).getOrCreate()
    // 导入隐式转换，这里的spark是环境对象
    // 要求这个对象必须使用val声明
    import spark.implicits._
//    spark.sql("create table bb(id int)")
//    spark.sql("show tables").show()
    // 使用外部Hive
    spark.sql("load data local inpath 'input/id.txt' into table aa")
    spark.sql("select * from aa").show()
    spark.stop()
  }

}
