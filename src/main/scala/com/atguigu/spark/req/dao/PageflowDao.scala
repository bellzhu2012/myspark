package com.atguigu.spark.req.dao

import com.atguigu.spark.req.bean.UserVisitAction
import com.atguigu.summer.framework.dao.TDao
import com.atguigu.summer.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/10 11:09
  * @Description:
  * @Modified By: lenovo
  */
class PageflowDao extends TDao {
  override def getResult(path:String) ={
    val rdd: RDD[String] = EnvUtil.getEnv().textFile(path)
    val rdd1: RDD[UserVisitAction] = rdd.map(
      line => {
        val words = line.split("_")
        val action = UserVisitAction(
          words(0),
          words(1).toLong,
          words(2),
          words(3).toLong,
          words(4),
          words(5),
          words(6).toLong,
          words(7).toLong,
          words(8),
          words(9),
          words(10),
          words(11),
          words(12).toLong
        )
        action
      }
    )
    rdd1
  }
}
