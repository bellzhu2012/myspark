package com.atguigu.spark.req.service

import com.atguigu.spark.req.bean.HCBean
import com.atguigu.spark.req.dao.HCATDao
import com.atguigu.summer.framework.service.TService
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/9 10:41
  * @Description:
  * @Modified By: lenovo
  */
class HCATService2 extends TService{
  private val dao = new HCATDao

  override def analysis()= {

    // 获取RDD
    val rdd: RDD[String] = dao.getResult("input2")
    rdd.cache()
    // 点击总数
    val rdd1= rdd.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), HCBean(datas(6), 1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => {
            (id, HCBean(id, 0, 1, 0))
          })
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => {
            (id, HCBean(id, 0, 0, 1))
          })
        } else {
          Nil
        }
      }
    )



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
    val rdd2: RDD[(String, HCBean)] = rdd1.reduceByKey(
      (t1, t2) => {
        val b1: HCBean = t1
        val b2: HCBean = t2
        b1.clickcount = b1.clickcount + b2.clickcount
        b1.ordercount = b1.ordercount + b2.ordercount
        b1.paycount = b2.paycount + b1.paycount
        b1
      }
    )

    val result = rdd2.collect().sortWith(
      (left, right) => {
        val leftHC = left._2
        val rightHC = right._2
        if (leftHC.clickcount > rightHC.clickcount) {
          true
        } else if (leftHC.clickcount == rightHC.clickcount) {
          if (leftHC.ordercount > rightHC.ordercount) {
            true
          } else if (leftHC.ordercount == rightHC.ordercount) {
            leftHC.paycount > rightHC.paycount
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10)
   println("service2")
    println(result.mkString(","))


    
  }
}
