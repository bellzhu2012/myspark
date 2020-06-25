package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

/**
  * @Author: lenovo
  * @Time: 2020/6/12 12:35
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL_AggregatorUDAF1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val rdd = spark.sparkContext.makeRDD(List(
      (1, "zhangsan", 30),
      (2, "lisi", 25),
      (3, "wangwu", 45)
    ))
    import spark.implicits._
    val df = rdd.toDF("id","name","age")
    val ds = df.as[User]
//    ds.show()
    val avgAge = new MyAggregatorUDAF
    //DSL
    val column: TypedColumn[User, Long] = avgAge.toColumn
    ds.select(column).show()

    spark.stop()
  }
  case class User(id:Int, name:String, age:Int)
  case class AgeBuffer(var age:Long, var count:Long)
  class MyAggregatorUDAF extends Aggregator[User,AgeBuffer,Long]{
    override def zero: AgeBuffer = AgeBuffer(0L,0L)

    override def reduce(buffer: AgeBuffer, user: User): AgeBuffer = {
      buffer.age = buffer.age + user.age
      buffer.count = buffer.count + 1
      buffer
    }

    override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
      b1.age = b1.age + b2.age
      b1.count = b1.count + b2.count
      b1
    }

    override def finish(reduction: AgeBuffer): Long = {
      reduction.age / reduction.count
    }

    override def bufferEncoder: Encoder[AgeBuffer] = Encoders.product
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
