package com.atguigu.sparkstream.day05.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}




object MyKafkaUtils {
  private val pro: Properties = PropertiesUtil.load("wordcount.properties")
  private val brokerlist: String = pro.getProperty("kafka.broker.list")
  private val groupid: String = pro.getProperty("group_id")

  private val kafka_pram = Map(
    (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist),
    (ConsumerConfig.GROUP_ID_CONFIG, groupid),
    (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
    (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  )
  def getKafkaStream(topic:String,ssc:StreamingContext): InputDStream[ConsumerRecord[String, String]] ={
    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic),kafka_pram)
    )
  }
}
