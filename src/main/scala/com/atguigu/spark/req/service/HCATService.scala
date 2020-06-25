package com.atguigu.spark.req.service

import com.atguigu.spark.req.dao.HCATDao
import com.atguigu.summer.framework.service.TService
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/9 10:41
  * @Description:
  * @Modified By: lenovo
  */
class HCATService extends TService{
  private val dao = new HCATDao

  override def analysis()= {
    val start = System.currentTimeMillis()
    // 获取RDD
    val rdd: RDD[String] = dao.getResult("input2")
    rdd.cache()
    // 点击总数
    val clickRDD1 = rdd.map(
      action => {
        val datas = action.split("_")
        (datas(7), 1)
      }
    )
    val clickRDD2 = clickRDD1.filter( _._1 != "-1")
    val categoryIdToClickCountRDD = clickRDD1.reduceByKey(_ + _)
//    println(categoryIdToClickCountRDD.take(3).mkString(","))
    // 下单总数
    val orderRDD1 = rdd.map(
      action => {
        val datas = action.split("_")
        datas(9)
      }
    ).filter( _ != "null")
    val orderRDD2 = orderRDD1.flatMap(
      data => {
        val ids = data.split(",")
        ids.map(
          id => (id, 1)
        )
      }
    )
    val categoryIdToOrderCountRDD = orderRDD2.reduceByKey(_ + _)
//    println(categoryIdToOrderCountRDD.take(3).mkString(","))

    // 支付总数
    val payRDD1 = rdd.map(
      action => {
        val datas = action.split("_")
        datas(11)
      }
    ).filter( _ != "null")
    val payRDD2 = payRDD1.flatMap(
      data => {
        val ids = data.split(",")
        ids.map(
          id => (id, 1)
        )
      }
    )
    val categoryIdToPayCountRDD = payRDD2.reduceByKey(_ + _)
//    println(categoryIdToPayCountRDD.take(3).mkString(","))

    // 汇总
    // (key,(clickcount, ordercount, paycount))
    val part1: RDD[(String, (Int, Int))] = categoryIdToClickCountRDD.join(categoryIdToOrderCountRDD)
    val part2: RDD[(String, ((Int, Int), Int))] = part1.join(categoryIdToPayCountRDD)
    val part3 = part2.mapValues {
      case ((click, order), pay) => {
        (click, order, pay)
      }
    }
    val result = part3.sortBy(_._2,false)
    println(result.take(5).mkString(","))
    val end = System.currentTimeMillis()
    println(end - start)
    
  }
}
