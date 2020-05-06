package com.atguigu.sparkstream.day04.transformations

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop103", 9999)

    socket.transform { rdd =>
      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    }.print

    ssc.start()
    ssc.awaitTermination()
  }
}
