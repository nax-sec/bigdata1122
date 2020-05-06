package com.atguigu.sparkstream.day05.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkstream.day05.bean.Ads_log
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

object LastHourAdCountHandler {

  private val sdf: SimpleDateFormat = new SimpleDateFormat("HH:mm")

  def getLastHourAdCount(ads: DStream[Ads_log]): DStream[(String, List[(String, Long)])] = {

    val winads: DStream[Ads_log] = ads.window(Minutes(1))

    val timeads: DStream[((String, String), Long)] = winads.map(adsLog => {
      val hm: String = sdf.format(new Date(adsLog.timestamp))
      ((adsLog.adid, hm), 1L)
    })

    val countads: DStream[((String, String), Long)] = timeads.reduceByKey(_ + _)


    val countads2: DStream[(String, (String, Long))] = countads.map { case ((ad, hm), count) =>
      (ad, (hm, count))
    }


    val resads: DStream[(String, List[(String, Long)])] = countads2.groupByKey()
      .mapValues(iter => {
        iter.toList.sortWith(_._1 < _._1)
      })

    resads
  }

}
