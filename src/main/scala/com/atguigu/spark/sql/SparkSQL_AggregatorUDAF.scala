package com.atguigu.spark.sql

import com.atguigu.spark.sql.SparkSQL_UDAF.MyUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql._

/**
  * @Author: lenovo
  * @Time: 2020/6/12 12:19
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL_AggregatorUDAF {
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

    val dataSet = frame.as[User]
    // DSL语言
    val avgAge = new MyAggregator
    val column: TypedColumn[User, Long] = avgAge.toColumn
    dataSet.select(column).show()
//    val newDS: Dataset[User] = dataSet.map(
//      user => {
//        User(user.id, "name:" + user.name, user.age)
//      }
//    )
//    newDS.show()
    // 使用自定义函数
//    spark.udf.register("addName", (x) => {"name:" + x})
//    spark.udf.register("changeAge",(y) => {"age:" + y})
    //    spark.sql("select addName(name),changeAge(age) from user").show()
    //  创建自定义UDAF
//    val avgAge = new MyUDAF
//    spark.udf.register("avgAge",avgAge)
//    spark.sql("select avgAge(age) from user").show()

    // 释放对象
    spark.stop
  }
  case class User(id:Int, name:String, age:Int)
  case class AgeBuffer(var age:Long, var count : Long)
  class MyAggregator extends Aggregator[User,AgeBuffer,Long]{
    override def zero: AgeBuffer = {
      AgeBuffer(0L,0L)
    }

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

    override def finish(buffer: AgeBuffer): Long = {
      buffer.age / buffer.count
    }

    override def bufferEncoder: Encoder[AgeBuffer] = {
      Encoders.product
    }

    override def outputEncoder: Encoder[Long] = {
      Encoders.scalaLong
    }
  }
}
