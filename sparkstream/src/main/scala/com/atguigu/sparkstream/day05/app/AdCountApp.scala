package com.atguigu.sparkstream.day05.app

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkstream.day05.bean.Ads_log
import com.atguigu.sparkstream.day05.handler.{AdCount, BlackListHandler}
import com.atguigu.sparkstream.day05.utils.{MyKafkaUtils, PropertiesUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AdCountApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc = new StreamingContext(conf, Seconds(3))

    val pro = PropertiesUtil.load("wordcount.properties")
    val topic = pro.getProperty("kafka.topic")

    val kds = MyKafkaUtils.getKafkaStream(topic, ssc)

    val ads = kds.map { record =>
      val fields = record.value().split(" ")
      Ads_log(fields(0).toLong, fields(1), fields(2), fields(3), fields(4))
    }


    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dtds = ads.map { adl: Ads_log =>
      val dt = sdf.format(new Date(adl.timestamp))
      ((dt, adl.area,adl.city, adl.adid), 1L)
    }

    AdCount.DayCount(dtds)


    ssc.start()
    ssc.awaitTermination()
  }
}
