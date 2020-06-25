package com.atguigu.spark.req.controller

import com.atguigu.spark.req.service._
import com.atguigu.summer.framework.controller.TController


/**
  * @Author: lenovo
  * @Time: 2020/6/9 10:39
  * @Description:
  * @Modified By: lenovo
  */
class HCATController extends TController{
  private val service = new HCATService4

  override def execute(): Unit = {
    service.analysis()
  }

}
