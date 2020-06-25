package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

/**
  * @Author: lenovo
  * @Time: 2020/6/12 12:47
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL_UDAF1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val rdd = spark.sparkContext.makeRDD(List(
      (1, "zhangsan", 20),
      (2, "lisi", 30),
      (3, "wang", 10)
    ))
    val df = rdd.toDF("id","name","age")
    df.createTempView("user")
    val f = new MyUDAF
    spark.udf.register("avgAge",f)
    spark.sql("select avgAge(age) from user").show()

  }
  case class User(id:Int, name:String, age:Int)
  class MyUDAF extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = {
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
