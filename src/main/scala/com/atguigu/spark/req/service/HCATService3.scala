package com.atguigu.spark.req.service

import com.atguigu.spark.req.bean
import com.atguigu.spark.req.dao.HCATDao
import com.atguigu.spark.req.helper.HCATAccumulator
import com.atguigu.summer.framework.service.TService
import com.atguigu.summer.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * @Author: lenovo
  * @Time: 2020/6/9 21:14
  * @Description:
  * @Modified By: lenovo
  */
class HCATService3 extends TService{
  private val dao = new HCATDao
  override def analysis(): Any = {
    val rdd: RDD[String] = dao.getResult("input2")
    rdd.cache()
    val accumulator = new HCATAccumulator
    EnvUtil.getEnv().register(accumulator)
    rdd.foreach(
      line =>{
        val words = line.split("_")
        if (words(6) != "-1"){
          accumulator.add((words(6),"click"))
        }else if(words(8) != "null"){
          val ids = words(8).split(",")
          ids.map(id =>{
            accumulator.add((id,"order"))
          })
        }else if(words(10) != "null"){
          val ids = words(10).split(",")
          ids.map(id =>{
            accumulator.add((id,"pay"))
          })
        }
      }
    )
    println("service3")
    val map: mutable.Map[String, bean.HCBean] = accumulator.value.take(10)
    println(map)
    map
  }
}
