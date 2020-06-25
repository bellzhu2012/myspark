package com.atguigu.spark.req.application
import com.atguigu.spark.req.controller.WordCountController
import com.atguigu.summer.framework.core.TApplication
import org.apache.spark.SparkContext
/**
  * @Author: lenovo
  * @Time: 2020/6/8 20:28
  * @Description:
  * @Modified By: lenovo
  */
object WordCountApplication extends App with TApplication{

    start("spark"){
        val controller = new WordCountController()
        controller.execute()

    }
}
