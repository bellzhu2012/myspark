package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * @Author: lenovo
  * @Time: 2020/6/12 10:24
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL02 {
  def main(args: Array[String]): Unit = {
    // 创建环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 逻辑操作
    // TODO SQL
    import spark.implicits._
    // RDD <==>DataFrame
    val rdd = spark.sparkContext.makeRDD(List(
      (1, "zhangsan", 20),
      (2, "liudan", 23),
      (3, "lisi", 25)
    ))
    val frame = rdd.toDF("id","name","age")
    frame.createTempView("user")
//    val newDF = frame.map(
//      row => {
//        val id = row(0)
//        val name = row(1)
//        val age = row(2)
//        Row(id, "name: " + name, age)
//      }
//    )
    val dataSet = frame.as[User]
    val newDS: Dataset[User] = dataSet.map(
      user => {
        User(user.id, "name:" + user.name, user.age)
      }
    )
    newDS.show()
    // 使用自定义函数
    spark.udf.register("addName", (x) => {"name:" + x})
    spark.udf.register("changeAge",(y) => {"age:" + y})
    spark.sql("select addName(name),changeAge(age) from user").show()

    // 释放对象
    spark.stop
  }
  case class User(id:Int, name:String, age:Int)

}
