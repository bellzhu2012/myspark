package com.atguigu.spark.req.dao

import com.atguigu.spark.req.bean.UserVisitAction
import com.atguigu.summer.framework.dao.TDao
import com.atguigu.summer.framework.util.EnvUtil
import com.google.protobuf.Descriptors.EnumValueDescriptor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/9 10:41
  * @Description:
  * @Modified By: lenovo
  */
class HCATDao extends TDao{
  private val context: SparkContext = EnvUtil.getEnv()
  override def getResult(path: String)= {
    val value: RDD[String] = context.textFile(path)
    value
  }

}
