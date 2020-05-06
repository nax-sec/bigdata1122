package com.atguigu.sparkstream.day05.handler

import com.atguigu.sparkstream.day05.utils.JdbcUtil
import org.apache.spark.streaming.dstream.DStream

object AdCount {
  def DayCount(ds:DStream[((String, String, String,String), Long)]) :Unit={
      ds.foreachRDD{rdd=>
        rdd.foreachPartition{iter=>
          val con = JdbcUtil.getconnection
          iter.foreach{case ((dt,area,city,adid),count)=>
            JdbcUtil.executeUpdate(con,
              """
                |insert into area_city_ad_count
                |values(?,?,?,?,?)
                |on duplicate key
                |update count=count+?
                |""".stripMargin,
            Array(dt,area,city,adid,count,count))
            val num: Long = JdbcUtil.getDataFromMysql(con,
              """
                |select count from area_city_ad_count where
                |dt=?
                |and
                |area=?
                |and
                |city=?
                |and
                |adid=?
                |""".stripMargin,
              Array(dt,area,city,adid)
            )
            println(dt+","+area+","+city+","+(count+num))
          }
          con.close()
        }
      }
  }
}
