package com.atguigu.spark.req.dao

import com.atguigu.summer.framework.dao.TDao
import com.atguigu.summer.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
  * @Author: lenovo
  * @Time: 2020/6/8 20:28
  * @Description:
  * @Modified By: lenovo
  */
class WordCountDao extends TDao{

  override def getResult(path :String)={
    val rdd: RDD[String] = EnvUtil.getEnv().textFile(path)
    val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = wordRDD.map( word=>(word,1) )
    val wordToSumRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    val wordCountArray: Array[(String, Int)] = wordToSumRDD.collect()
    wordCountArray
  }
}
