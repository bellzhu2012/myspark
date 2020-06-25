package com.atguigu.spark.req.service

import com.atguigu.spark.req.dao.WordCountDao
import com.atguigu.summer.framework.service.TService
import com.atguigu.summer.framework.util.EnvUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/8 20:35
  * @Description:
  * @Modified By: lenovo
  */
class WordCountService() extends TService{
  private val dao = new WordCountDao
  override def analysis() = {
    val array: Array[(String, Int)] = dao.getResult("input/wordcount.txt")
   array
  }
}
