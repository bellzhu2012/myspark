package com.atguigu.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 9:19
  * @Description:
  * @Modified By: lenovo
  */
object Spark52_Serial {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
    // 如果算子中使用了算子外的对象，那么在执行时，需要保证此对象可以序列化
//    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
//    val user = new User
//    rdd.foreach(
//      num => {
//        println("age = " + (user.age + num))
//      }
//    )
    // Spark的算子的操作其实都是闭包，所以闭包有可能包含外部的变量
    // 如果包含外部的变量，那么就一定要保证这个外部变量可序列化
    // 所以Spakr在提交作业之前，应该对比包内的变量进行检测，检测是否能够序列化
    // 这个操作称为闭包检测
    // SparkEnv会产生一个序列化对象，对匿名函数进行序列化
//  def foreach(f: T => Unit): Unit = withScope {
//    val cleanF = sc.clean(f) （闭包检查）
//    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
//  }
    val user = new User
    val rdd: RDD[Int] = sc.makeRDD(List())
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )
    sc.stop()
  }
//  class User {
//    val age:Int = 20
//  }
  class User extends Serializable{
    val age:Int = 20
  }
}
