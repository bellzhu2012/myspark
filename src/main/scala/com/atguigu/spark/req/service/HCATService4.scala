package com.atguigu.spark.req.service

import com.atguigu.spark.req.bean.{HCBean, UserVisitAction}
import com.atguigu.spark.req.dao.HCATDao
import com.atguigu.spark.req.helper.HCATAccumulator
import com.atguigu.summer.framework.service.TService
import com.atguigu.summer.framework.util.EnvUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * @Author: lenovo
  * @Time: 2020/6/9 21:14
  * @Description:
  * @Modified By: lenovo
  */
class HCATService4 extends TService{
  private val dao = new HCATDao
  override def analysis(): Any = {
    val service3 = new HCATService3
    val map : mutable.Map[String, HCBean] = service3.analysis().asInstanceOf[mutable.Map[String, HCBean]]
    val list: List[HCBean] = map.toList.map {
      case (id, bean) => {
        bean
      }
    }
    val context: SparkContext = EnvUtil.getEnv()
    val bcList: Broadcast[List[HCBean]] = context.broadcast(list)
    println(map.size)
    // 获取Top10每个品类的session
    val rdd : RDD[String] = dao.getResult("input2")
    val rdd1: RDD[UserVisitAction] = rdd.map(
      line => {
        val words = line.split("_")
        val action = UserVisitAction(
          words(0),
          words(1).toLong,
          words(2),
          words(3).toLong,
          words(4),
          words(5),
          words(6).toLong,
          words(7).toLong,
          words(8),
          words(9),
          words(10),
          words(11),
          words(12).toLong
        )
        action
      }
    )
    val rdd2: RDD[UserVisitAction] = rdd1.filter(
      action => {
        var flag = false
        bcList.value.foreach(
          bean => {
//            if (action.click_category_id == bean.id.toLong) {
//              flag = true
//            } else {
//              flag = false
//            }
            flag = bean.id.contains(action.click_category_id.toString)
          }
        )
        flag
      }
    )
    println(rdd2.collect().mkString(","))
    println("service4")

  }
}
