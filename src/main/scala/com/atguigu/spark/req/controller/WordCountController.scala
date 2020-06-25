package com.atguigu.spark.req.controller

import com.atguigu.spark.req.service.WordCountService
import com.atguigu.summer.framework.controller.TController
import org.apache.spark.SparkContext

/**
  * @Author: lenovo
  * @Time: 2020/6/8 20:32
  * @Description:
  * @Modified By: lenovo
  */
class WordCountController() extends TController{
  private val service = new WordCountService()

  override def execute(): Unit = {
    val result: Array[(String, Int)] = service.analysis()
    println(result.mkString(","))
  }
}
