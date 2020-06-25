package com.atguigu.spark.req.application

import com.atguigu.spark.req.controller.HCATController
import com.atguigu.summer.framework.core.TApplication

/**
  * @Author: lenovo
  * @Time: 2020/6/9 10:37
  * @Description:
  * @Modified By: lenovo
  */
object HotCategoryAnalysisTop10Application extends App with TApplication{

  // 热门商品前十排名程序
  start("spark"){
    val controller = new HCATController
    controller.execute()
  }

}
