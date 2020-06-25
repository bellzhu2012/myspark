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
class HCATService1 extends TService{
  private val dao = new HCATDao

  override def analysis()= {
    val start = System.currentTimeMillis()
    // 获取RDD
    val rdd: RDD[String] = dao.getResult("input2")
    rdd.cache()
    // 点击总数
    val rdd1: RDD[(String,(Int,Int,Int))] = rdd.map(
      line => {
        val words = line.split("_")
        if (words(7) != "-1") {
          // 点击事件
          (words(7), (1, 0, 0))
        } else if (words(9) != "null") {
          // 订单事件
          (words(9), (0, 1, 0))
        } else {
          // 支付事件
          (words(11), (0, 0, 1))
        }
      })


//    val clickRDD1 = rdd.map(
//      action => {
//        val datas = action.split("_")
//        (datas(7), (1,0,0))
//      }
//    )
//    val clickRDD2 = clickRDD1.filter( _._1 != "-1")
//    val categoryIdToClickCountRDD = clickRDD2.reduceByKey{
//      case ((click1,order1,pay1),(click2, order2, pay2)) =>{
//        (click1 + click2, order1 + order2, pay1 + pay2)
//      }
//    }
////    val categoryIdToClickCountRDD = clickRDD1.reduceByKey(_ + _)
////    println(categoryIdToClickCountRDD.take(3).mkString(","))
//    // 下单总数
//    val orderRDD1 = rdd.map(
//      action => {
//        val datas = action.split("_")
//        datas(9)
//      }
//    ).filter( _ != "null")
//    val orderRDD2 = orderRDD1.flatMap(
//      data => {
//        val ids = data.split(",")
//        ids.map(
//          id => (id, (0,1,0))
//        )
//      }
//    )
//    val categoryIdToOrderCountRDD = orderRDD2.reduceByKey{
//      case ((click1,order1,pay1),(click2, order2, pay2)) =>{
//        (click1 + click2, order1 + order2, pay1 + pay2)
//      }
//    }
////    println(categoryIdToOrderCountRDD.take(3).mkString(","))
//
//    // 支付总数
//    val payRDD1 = rdd.map(
//      action => {
//        val datas = action.split("_")
//        datas(11)
//      }
//    ).filter( _ != "null")
//    val payRDD2 = payRDD1.flatMap(
//      data => {
//        val ids = data.split(",")
//        ids.map(
//          id => (id, (0,0,1))
//        )
//      }
//    )
//    val categoryIdToPayCountRDD = payRDD2.reduceByKey{
//      case ((click1,order1,pay1),(click2, order2, pay2)) =>{
//        (click1 + click2, order1 + order2, pay1 + pay2)
//      }
//    }
//    println(categoryIdToPayCountRDD.take(3).mkString(","))

    // 汇总
    // (key,(clickcount, ordercount, paycount))
//    val part1: RDD[(String, (Int, Int))] = categoryIdToClickCountRDD.join(categoryIdToOrderCountRDD)
    //    val part2: RDD[(String, ((Int, Int), Int))] = part1.join(categoryIdToPayCountRDD)
    //    val part3: RDD[(String, (Int, Int, Int))] = part2.mapValues {
    //      case ((click, order), pay) => {
    //        (click, order, pay)
    //      }
    //    }
//    val unionRDD = categoryIdToClickCountRDD.union(categoryIdToOrderCountRDD).union(categoryIdToPayCountRDD)
    val part3 = rdd1.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val result = part3.sortBy(_._2,false)
    println(result.take(5).mkString(","))
    val end = System.currentTimeMillis()
    println(end - start)
    
  }
}
