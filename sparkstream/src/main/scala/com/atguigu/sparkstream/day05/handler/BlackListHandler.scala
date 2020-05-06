package com.atguigu.sparkstream.day05.handler

import java.sql.Connection

import com.atguigu.sparkstream.day05.bean.Ads_log
import com.atguigu.sparkstream.day05.utils.JdbcUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream

object BlackListHandler {

  def saveToMysql(ds:DStream[((String, String, String), Long)]): Unit = {
    ds.foreachRDD{ rdd=>
      rdd.foreachPartition{ iter =>
        val connection: Connection = JdbcUtil.getconnection
        iter.foreach{ case ((dt, user, ad), count)=>
          JdbcUtil.executeUpdate(
            connection,
            """
              |insert into user_ad_count
              |values(?,?,?,?)
              |on duplicate key
              |update count=count+?
              |""".stripMargin,
            Array(dt,user,ad,count,count)
          )
        }
        connection.close()
      }
      val con = JdbcUtil.getconnection
      JdbcUtil.executeUpdate(
        con,
        """
          |insert into black_list(userid)
          |select userid from user_ad_count where count >= 100
          |on duplicate key
          |update black_list.userid=black_list.userid
          |""".stripMargin,
        Array()
      )
      con.close()
    }
  }

  def filterBlackList(sc:SparkContext,ds:DStream[Ads_log]): DStream[Ads_log] ={
    val con = JdbcUtil.getconnection
    val blacklist = JdbcUtil.getBlackList(con)
    con.close()
    val listBC: Broadcast[List[String]] = sc.broadcast(blacklist)
    ds.filter(ads_log => !listBC.value.contains(ads_log.userid))
  }
}
