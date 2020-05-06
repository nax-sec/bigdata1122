package com.atguigu.sparkstream.day04.output

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object FeachRdd {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop103", 9999)

    socket.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreachRDD{
      rdd => rdd.foreach(println)
    }

    ssc.start

    ssc.awaitTermination
  }
}
