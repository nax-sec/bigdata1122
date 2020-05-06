package com.atguigu.sparkstream.day04.kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object Direct010 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))


    val map: Map[String, Object] = Map(
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop103:9092,hadoop104:9092,hadoop:9092"),
      (ConsumerConfig.GROUP_ID_CONFIG, "bigdata1122"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    )

    val ds: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set("test", "test2"), map)
    )

    ds.flatMap(_.value.split(" ")).map((_,1)).reduceByKey(_+_).print

    ssc.start()

    ssc.awaitTermination()
  }
}
