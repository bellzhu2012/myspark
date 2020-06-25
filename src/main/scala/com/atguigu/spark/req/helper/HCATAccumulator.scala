package com.atguigu.spark.req.helper

import com.atguigu.spark.req.bean.HCBean
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @Author: lenovo
  * @Time: 2020/6/9 21:17
  * @Description:
  * @Modified By: lenovo
  */
class HCATAccumulator extends AccumulatorV2[(String,String),mutable.Map[String, HCBean]]{
  private var map: mutable.Map[String, HCBean] = mutable.Map[String,HCBean]()
  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HCBean]] = {
    new HCATAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: (String, String)): Unit = {
    val id = v._1
    val action = v._2
    val bean = map.getOrElse(id, HCBean(id,0,0,0))
    action match {
      case "click" => bean.clickcount += 1
      case "order" => bean.ordercount += 1
      case "pay" => bean.paycount += 1
      case _ =>
    }
    map(id) = bean
  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HCBean]]): Unit = {
    other.value.foreach{
      case (id, bean) => {
        val tempBean = map.getOrElse(id,HCBean(id,0,0,0))
        tempBean.clickcount += bean.clickcount
        tempBean.ordercount += bean.ordercount
        tempBean.paycount += bean.ordercount
        map(id) = tempBean
      }
    }
  }

  override def value: mutable.Map[String, HCBean] = {
    map
  }
}
