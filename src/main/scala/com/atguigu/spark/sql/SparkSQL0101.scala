package com.atguigu.spark.sql

import com.atguigu.spark.sql.SparkSQL01.User
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author: lenovo
  * @Time: 2020/6/10 22:07
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL0101 {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 逻辑
    val df: DataFrame = spark.read.json("input/user.json")
//    ds.show()
    df.createTempView("user")
    spark.sql("select * from user").show
    // DSL
    df.select("username").show()
    import spark.implicits._
    df.select('age).show()
    val rdd = spark.sparkContext.makeRDD(List(
      (1, "zhangsan", 20),
      (2, "lisi", 25),
      (3, "liuda", 30)
    ))

    //TODO RDD <==> DataFrame
    val rddToDataFrame: DataFrame = rdd.toDF("id","name","age")
    val DataFrameToRDD = rddToDataFrame.rdd
    //TODO RDD <==> DataSet
    val mapRDD = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val rddToDataSet = mapRDD.toDS()
    val DataSetToRDD = rddToDataSet.rdd
    //TODO DataFrame <==> DataSet
    val frameToSet = rddToDataFrame.as[User]
    val setToFrame = frameToSet.toDF()
    // 环境关闭
    spark.stop()
  }
}
