package com.atguigu.spark.sql

import com.atguigu.spark.sql.SparkSQL01.User
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @Author: lenovo
  * @Time: 2020/6/11 10:10
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL010101 {
  def main(args: Array[String]): Unit = {
    // TODO 初始化环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 逻辑
    // TODO 读取文件，生成DataFrame
    val df: DataFrame = spark.read.json("input/user.json")
//    df.show()
    // 临时视图
    df.createTempView("user")
    // 使用sql语言查询
//    spark.sql("select * from user").show()
    // 使用DSL语言
//    df.select("username").show()
    // 使用单引号,需要使用隐式转换，要求spark对象是val类型
    import spark.implicits._
//    df.select('age).show()
    //TODO RDD <==> DataFrame
    val rdd = spark.sparkContext.makeRDD(List(
      (1, "zhangsan", 20),
      (2, "lisi", 20),
      (3, "liuda", 20)
    ))
    val rddToDF = rdd.toDF("id","name","age")
    val DF2RDD = rddToDF.rdd
//    rddToDF.show()
    //TODO RDD <==> DataSet
    val mapRDD = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val mapRDDToDS: Dataset[User] = mapRDD.toDS()
    mapRDDToDS.show()
    val DS2RDD = mapRDDToDS.rdd
    //TODO DataFrame <==> DataSet
    val DF2DS = rddToDF.as[User]
    val DS2DF = mapRDDToDS.toDF()
    // TODO 环境关闭
    spark.stop()
  }
  case class User(id:Int, name:String, age:Int)
}
