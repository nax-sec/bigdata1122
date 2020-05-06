package com.atguigu.sparkstream.day04.stop

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object Stop {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop103", 9999)

    socket.transform(_.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)).print


    new Thread(new MonitorStop(ssc)).start()

    ssc.start

    ssc.awaitTermination
  }
}
