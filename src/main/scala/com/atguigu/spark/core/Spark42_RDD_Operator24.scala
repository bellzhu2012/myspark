package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/6 21:12
  * @Description:
  * @Modified By: lenovo
  */
object Spark42_RDD_Operator24 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
//    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("c",3),("b",2)))
    // key-value类型：sortByKey方法通过key进行排序
//    val rdd1: RDD[(String, Int)] = rdd.sortByKey()
    // 自定义类型排序
    val rdd = sc.makeRDD(List(
      (new User("l",4), 1),
      (new User("y",4), 2),
//      (new User("zhangsan",2), 3),
      (new User("x",4), 4)
    ),1)
    val rdd1 = rdd.sortByKey()
    println(rdd1.collect().mkString(","))
    // (name = zhangsan, age = 2,3),(name = zhangsan, age = 4,1),(name = lisi, age = 4,2)
    sc.stop()
  }
  // 如果自定义key进行排序，需要将key混入特质Ordered
  class User(var name:String, var age : Int) extends Ordered[User] with Serializable {
    override def compare(that: User): Int = {
//      if(this.age != that.age){
//        this.age - that.age
//      }else{
//        if(name > that.name){
//          -1
//        }else if(name == that.name){
//          0
//        }else{
//          1
//        }
//      }
      var cmd = this.age compareTo(that.age)
      if(cmd == 0){
        cmd = this.name compareTo(that.name)
      }
      cmd
    }

    override def toString: String = {
      s"name = $name, age = $age"
    }
  }
}
