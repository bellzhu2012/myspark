package com.atguigu.spark.review1

/**
  * @Author: lenovo
  * @Time: 2020/6/7 13:21
  * @Description:
  * @Modified By: lenovo
  */
object Spark06_RDD_File_PartitionData {
  def main(args: Array[String]): Unit = {
    // 1. Spark读取文件采用的是Hadoop的读取规则
    //    文件切片规则 :  以字节方式来切片
    //    数据读取规则 ： 以行为单位来读取

    // 2. 问题
    //    TODO 文件到底切成几片（分区的数量）?
    //    文件字节数（10），预计切片数量（2）
    //    10 / 2 => 5byte
    // totalSize = 10
    // goalSize = totalSize / numSplits = 10 / 2 = 5 ...1 => 3
    //   所谓的最小分区数，取决于总的字节数是否能整除分区数并且剩余的字节达到一个比率
    //   实际产生的分区数量可能大于最小分区数

    //   TODO 分区的数据如何存储？
    //   分区数据是以行为单位读取的是，而不是字节
  }
}
