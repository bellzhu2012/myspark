package com.atguigu.spark.req.service

import com.atguigu.spark.req.bean
import com.atguigu.spark.req.bean.UserVisitAction
import com.atguigu.spark.req.dao.{HCATDao, PageflowDao}
import com.atguigu.summer.framework.service.TService
import jdk.nashorn.internal.runtime.UserAccessorProperty
import org.apache.spark.rdd.RDD


/**
  * @Author: lenovo
  * @Time: 2020/6/10 11:07
  * @Description:
  * @Modified By: lenovo
  */
class PageflowService extends TService{
  private val dao = new PageflowDao
  override def analysis(): Any = {
    val actionRDD: RDD[bean.UserVisitAction] = dao.getResult("input2")
    actionRDD.cache()

    //规定统计有效的跳转规则
    // 1,2,3,4,5,6,7 -- 有效页面
    // 1-2,2-3,3-4,4-5,5-6,6-7 -- 有效跳转
    val flowIds = List(1,2,3,4,5,6,7)
    val okFlowIds: List[String] = flowIds.zip(flowIds.tail).map(tuple => tuple._1 + "-" + tuple._2)

    // 计算分母（统计所有page_id）
    val pageToOneRDD: RDD[(Long, Int)] = actionRDD.map(
      action => (action.page_id, 1)
    )
    val pageToSumRDD: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_ + _)
    val pageCountArray: Array[(Long, Int)] = pageToSumRDD.collect()


    // 计算分子
    // TODO 将数据分局用户Session进行分组
//    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(_.session_id)
    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] =
    actionRDD.groupBy(_.session_id)


    val pageflowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(
      iter => {
        // TODO 将分组后的数据根据时间进行排序
        val actions: List[bean.UserVisitAction] = iter.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        // TODO 将排序后的数据进行结构的转换
        //  action => pageid
        val pageids: List[Long] = actions.map(_.page_id)

        // TODO 将转换后的结果进行格式的转换
        // 1，2，3，4
        // 2，3，4
        // （1-2），（2-3），（3-4）
        val zipids: List[(Long, Long)] = pageids.zip(pageids.tail)
        // （（1-2），1），（（2-3），1），（（3，4），1）
        zipids.map {
          case (pageid1, pageid2) => {
            (pageid1 + "-" + pageid2, 1)
          }
        }.filter{
          case (ids, count) => {
            okFlowIds.contains(ids)
          }
        }
      }
    )


// TODO 将分组后的数据进行结构的转换
val pageidSumRDD: RDD[List[(String, Int)]] = pageflowRDD.map(_._2)
    // (1-2,1)
    val pageflowRDD1: RDD[(String, Int)] = pageidSumRDD.flatMap(list=>list)
    // (1-2,sum)
    val pageflowToSumRDD = pageflowRDD1.reduceByKey(_+_)

    pageflowToSumRDD.foreach{
      case ( pageflow, sum ) => {
        val pageid = pageflow.split("-")(0)
        val value = pageCountArray.toMap.getOrElse(pageid.toLong,1)
        println("页面跳转【"+pageflow+"】的转换率为: " + (sum.toDouble / value))
      }
    }
  }

  def analysis1(): Any = {
    val actionRDD: RDD[bean.UserVisitAction] = dao.getResult("input2")
    actionRDD.cache()
    // 计算分母（统计所有page_id）
    val pageToOneRDD: RDD[(Long, Int)] = actionRDD.map(
      action => (action.page_id, 1)
    )
    val pageToSumRDD: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_ + _)
    val pageCountArray: Array[(Long, Int)] = pageToSumRDD.collect()


    // 计算分子
    // TODO 将数据分局用户Session进行分组
    //    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(_.session_id)
    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] =
    actionRDD.groupBy(_.session_id)


    val pageflowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(
      iter => {
        // TODO 将分组后的数据根据时间进行排序
        val actions: List[bean.UserVisitAction] = iter.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        // TODO 将排序后的数据进行结构的转换
        //  action => pageid
        val pageids: List[Long] = actions.map(_.page_id)

        // TODO 将转换后的结果进行格式的转换
        // 1，2，3，4
        // 2，3，4
        // （1-2），（2-3），（3-4）
        val zipids: List[(Long, Long)] = pageids.zip(pageids.tail)
        // （（1-2），1），（（2-3），1），（（3，4），1）
        zipids.map {
          case (pageid1, pageid2) => {
            (pageid1 + "-" + pageid2, 1)
          }
        }
      }
    )


    // TODO 将分组后的数据进行结构的转换
    val pageidSumRDD: RDD[List[(String, Int)]] = pageflowRDD.map(_._2)
    // (1-2,1)
    val pageflowRDD1: RDD[(String, Int)] = pageidSumRDD.flatMap(list=>list)
    // (1-2,sum)
    val pageflowToSumRDD = pageflowRDD1.reduceByKey(_+_)

    pageflowToSumRDD.foreach{
      case ( pageflow, sum ) => {
        val pageid = pageflow.split("-")(0)
        val value = pageCountArray.toMap.getOrElse(pageid.toLong,1)
        println("页面跳转【"+pageflow+"】的转换率为: " + (sum.toDouble / value))
      }
    }
  }

  def analysis2(): Any = {
    val actionRDD: RDD[bean.UserVisitAction] = dao.getResult("input2")
    actionRDD.cache()
    // 统计所有page_id的访问次数
    val pageToOneRDD: RDD[(Long, Int)] = actionRDD.map(
      action => (action.page_id, 1)
    )
    val pageToSumRDD: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_ + _)
    val pageCountArray: Array[(Long, Int)] = pageToSumRDD.collect()


    // 统计页面停留时间
    // TODO 将数据分局用户Session进行分组
    //    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(_.session_id)
    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(_.session_id)
    // 将bean对象转化为只保留action_time
    val sessionToPageIDAndTimeRDD: RDD[(String, List[(Long, Long)])] = sessionRDD.mapValues(
      iter => {
        // 单次session中的操作按时间排序
        val actions = iter.toList.sortWith(
          (leftBean, rightBean) => {
            leftBean.action_time < rightBean.action_time
          }
        )
        // 排好序后，将List(b1,b2,...)转换为List((id1, time1),(di2,time2),...)
        val idToTimeList: List[(Long, Long)] = actions.map(
          bean => (bean.page_id, bean.getAction_time())
        )
        val tail: List[(Long, Long)] = idToTimeList.tail
        val tuples: List[((Long, Long), (Long, Long))] = idToTimeList.zip(tail)
        val pageidToTime: List[(Long, Long)] = tuples.map {
          case ((id1, time1), (id2, time2)) => {
            (id2, time2 - time1)
          }
        }
        pageidToTime
      }
    )
    // 将RDD[(String, List[(Long, Long)])] 转化为List[（long,Long）]
    val pageidToTime: RDD[List[(Long, Long)]] = sessionToPageIDAndTimeRDD.map(
      item => item._2
    )
    // 扁平化
    val pageidToTimeTuplesRDD: RDD[(Long, Long)] = pageidToTime.flatMap(list => list)

    // 分组聚合时间
    val finalPageidToTimeRDD: RDD[(Long, Long)] = pageidToTimeTuplesRDD.reduceByKey(_ + _)
    finalPageidToTimeRDD.foreach{
      case (id, totaltime) =>{
        val idToCountMap: Map[Long, Int] = pageCountArray.toMap
        val count: Int = idToCountMap.getOrElse(id,1)
        println("页面：" + id + ",点击次数：" + count + ",停留平均时间：" + (totaltime.toDouble/ count / 1000) + "秒")
      }
    }

  }
}
