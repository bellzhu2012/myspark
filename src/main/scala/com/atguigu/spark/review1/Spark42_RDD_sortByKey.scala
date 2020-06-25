package com.atguigu.spark.review1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lenovo
  * @Time: 2020/6/7 19:56
  * @Description:
  * @Modified By: lenovo
  */
object Spark42_RDD_sortByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)
//    val rdd = sc.makeRDD(
//      List(
//        ("a", 1), ("c", 3), ("b", 4)
//      )
//    )
    val rdd = sc.makeRDD(
      List(
        (new User("xuan", 1), 2),
        (new User("zhangsan", 1), 4),
        (new User("xuan", 3), 2),
        (new User("lisi", 3), 2)
      )
    )
    val mapRDD = rdd.map(tuple => tuple)

    val rdd1 = mapRDD.sortByKey()
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
  class User(var name : String, var age : Int) extends Ordered[User] with Serializable{
    override def compare(that: User): Int = {
      var result = this.age compareTo(that.age)
      if(result == 0){
        result = this.name compareTo(that.name)
      }
      result
    }
    override def toString = {
      s"name = $name, age = $age"
    }
  }
}
