package com.atguigu.spark.sql

import com.atguigu.spark.sql.Spark16_Test.CityRemarkUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
  * @Author: lenovo
  * @Time: 2020/6/13 10:33
  * @Description:
  * @Modified By: lenovo
  */
object SparkSQL15_Req1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建

    // TODO 访问外置的Hive
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .config(sparkConf).getOrCreate()
    import spark.implicits._
    spark.sql("use atguigu200213")
    spark.sql(
      """
        |select
        |   a.*,
        |   c.area,
        |   p.product_name,
        |   c.city_name
        |from user_visit_action a
        |join city_info c on c.city_id = a.city_id
        |join product_info p on p.product_id = a.click_product_id
        |where a.click_product_id > -1
      """.stripMargin
    ).createOrReplaceTempView("t1")

    // TODO 将数据根据区域和商品进行分组，统计商品点击的数量
    // 上海，北京，北京，深圳
    // in:cityname->String
    // buffer:2结构->(total, map)
    // out:remark->String
    // (商品点击总和，（城市，每个城市对应点击总和)）
    // （商品点击总和，Map（城市，点击Sum））
    // 城市点击Sum / 商品点击总和 %
    val fun = new CityRemarkUDAF
    spark.udf.register("cityRemark",fun)
    spark.sql(
      """
        |select
        |	    area,
        |	    product_name,
        |	    count(*) as clickCount,
        |    cityRemark(city_name)
        |from t1
        |group by area, product_name
      """.stripMargin).createOrReplaceTempView("t2")
//    // TODO 创建自定义聚合函数
//    val udaf = new CityRemarkUDAF
//    // TODO 注册聚合函数
//    spark.udf.register("cityRemark", udaf)
//
//    spark.sql(
//      """
//        |select
//        |    area,
//        |    product_name,
//        |    count(*) as clickCount,
//        |    cityRemark(city_name)
//        |from t1 group by area, product_name
//      """.stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        |	*,
        |	rank() over( partition by area order by clickCount desc ) as rank
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")

    spark.sql(
      """
        |select
        |	*
        |from t3
        |where rank <=3
      """.stripMargin).show()


    spark.stop()
  }
  class CityRemarkUDAF extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = {
      StructType(Array(StructField("cityName",StringType)))
    }

    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("totalCount",LongType),
        StructField("cityMap",MapType(StringType,LongType))
      ))
    }

    override def dataType: DataType = {
      StringType
    }

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = Map[String, Long]()
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val cityName = input.getString(0)
      buffer(0) = buffer.getLong(0) + 1
      val cityMap = buffer.getMap[String, Long](1)
      val newValue = cityMap.getOrElse(cityName,0L) + 1
      buffer(1) = cityMap.updated(cityName,newValue)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      var map1 = buffer1.getMap[String,Long](1)
      var map2 = buffer2.getMap[String,Long](1)
      map1.foldLeft(map2){
        case (map, (key,value)) => {
          map.updated(key,map.getOrElse(key,0L) + value)
        }
      }

    }

    override def evaluate(buffer: Row): Any = {
      val totalCount: Long = buffer.getLong(0)
      val cityMap = buffer.getMap[String, Long](1)
      val cityToCountList = cityMap.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(2)
      val hasRest = cityMap.size > 2
      var rest = 0L
      val s = new StringBuilder
      cityToCountList.foreach{
        case (city,count) => {
          val r = (count * 100) / totalCount
          s.append(city + " " + r + "%,")
          rest = rest + r
        }
      }

      if (hasRest){
        s.toString() + "其他" + (100 - rest) + "%"
      }else{
        s.toString().substring(0,s.size)
      }
    }
  }
}
