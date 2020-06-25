package com.atguigu.spark.req.controller

import com.atguigu.spark.req.service.PageflowService
import com.atguigu.summer.framework.controller.TController

/**
  * @Author: lenovo
  * @Time: 2020/6/10 11:29
  * @Description:
  * @Modified By: lenovo
  */
class PageflowController extends TController{
  private val service = new PageflowService
  override def execute(): Unit = {
    service.analysis2()
  }
}
