package com.atguigu.spark.req

import java.text.SimpleDateFormat
import java.util.Date

/**
  * @Author: lenovo
  * @Time: 2020/6/9 14:51
  * @Description:
  * @Modified By: lenovo
  */
package object bean {

    case class HCBean(id: String, var clickcount: Int, var ordercount: Int, var paycount: Int)

    case class UserVisitAction(
          date : String,
          user_id : Long,
          session_id : String,
          page_id : Long,
          action_time : String,
          search_keyword : String,
          click_category_id : Long,
          click_product_id : Long,
          order_category_ids : String,
          order_product_ids : String,
          pay_category_ids : String,
          pay_product_ids : String,
          city_id : Long
    ){
        def getAction_time()={
            val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val date: Date = format.parse(action_time)
            date.getTime()
        }
    }
}
