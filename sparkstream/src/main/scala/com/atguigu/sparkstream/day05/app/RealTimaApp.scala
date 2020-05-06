package com.atguigu.sparkstream.day05.app

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.atguigu.sparkstream.day05.bean.Ads_log
import com.atguigu.sparkstream.day05.handler.BlackListHandler
import com.atguigu.sparkstream.day05.utils.{MyKafkaUtils, PropertiesUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimaApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc = new StreamingContext(conf, Seconds(3))

    val pro: Properties = PropertiesUtil.load("wordcount.properties")
    val topic = pro.getProperty("kafka.topic")

    val kds = MyKafkaUtils.getKafkaStream(topic, ssc)

    val ads = kds.map { record =>
      val fields: Array[String] = record.value().split(" ")
      Ads_log(fields(0).toLong, fields(1), fields(2), fields(3), fields(4))
    }

    val fds = BlackListHandler.filterBlackList(ssc.sparkContext, ads)

    fds.print

    fds.cache()

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val dtds: DStream[((String, String, String), Long)] = fds.map(adsLog => {
      val dt: String = sdf.format(new Date(adsLog.timestamp))
      ((dt, adsLog.userid, adsLog.adid), 1L)
    }).reduceByKey(_ + _)

    BlackListHandler.saveToMysql(dtds)

    ssc.start()

    ssc.awaitTermination()


  }
}
