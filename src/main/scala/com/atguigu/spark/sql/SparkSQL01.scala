package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author: lenovo
  * @Time: 2020/6/10 21:36
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL01 {
  def main(args: Array[String]): Unit = {
    // 创建环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 逻辑操作
    // TODO SQL
    val df: DataFrame = spark.read.json("input/user.json")
    // 将df转换为视图
    val view: Unit = df.createTempView("user")
    spark.sql("select * from user").show()
    // DSL
    import spark.implicits._
    df.select("username").show()
    df.select($"age",$"username").show()
    // 如果查询的列名采用单引号，那么需要隐式转换
    // 导入spark对象的方法，需要是val声明

    df.select('username,'age).show()
    // RDD <==>DataFrame
    val rdd = spark.sparkContext.makeRDD(List(
      (1, "zhangsan", 20),
      (2, "liudan", 23),
      (3, "lisi", 25)
    ))
    val frame = rdd.toDF("id","name","age")
    println("=====DataFrame=====")
    frame.show()
    val frameToRDD = frame.rdd

    // RDD <==> DataSet
    val mapRDD = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val rddToDS = mapRDD.toDS()
    println("=====DataSet=====")
    rddToDS.show()
    val DSToRDD = rddToDS.rdd
    // DS <==> DF
    println("DS <==> DF")
    val frameToDS = frame.as[User]
    frameToDS.show()
    val DSToDS = frameToDS.toDF()
    // 释放对象
    spark.stop
  }
  case class User(id:Int, name:String, age:Int)
}
