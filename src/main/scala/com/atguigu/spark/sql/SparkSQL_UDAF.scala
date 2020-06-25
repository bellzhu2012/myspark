package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * @Author: lenovo
  * @Time: 2020/6/12 11:27
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL_UDAF {
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
      (2, "liudan", 24),
      (3, "lisi", 28)
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
//    spark.sql("select addName(name),changeAge(age) from user").show()
    //  创建自定义UDAF
    val avgAge = new MyUDAF
    spark.udf.register("avgAge",avgAge)
    spark.sql("select avgAge(age) from user").show()

    // 释放对象
    spark.stop
  }
  case class User(id:Int, name:String, age:Int)
  class MyUDAF extends UserDefinedAggregateFunction{override def inputSchema: StructType = {
    StructType(Array(StructField("age",LongType)))
  }

    override def bufferSchema: StructType = {
      StructType(Array(StructField("age",LongType),StructField("count",LongType)))
    }

    override def dataType: DataType = {
      LongType
    }

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L

    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }
}
