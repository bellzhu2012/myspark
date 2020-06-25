package com.atguigu.spark.req.application

import com.atguigu.spark.req.controller.PageflowController
import com.atguigu.summer.framework.core.TApplication

/**
  * @Author: lenovo
  * @Time: 2020/6/10 11:31
  * @Description:
  * @Modified By: lenovo
  */
object PageflowApplication extends App with TApplication{
  start("spark"){
    val controller = new PageflowController
    controller.execute()
  }

}
