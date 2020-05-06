package com.atguigu.sparkstream.day04.transformations

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object Join {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop103", 9999)

    val socket2: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop103", 8888)

    val t1: DStream[(String, Int)] = socket.transform(_.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _))
    val t2: DStream[(String, Int)] = socket2.transform(_.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _))

    t1.join(t2).print

    ssc.start
    ssc.awaitTermination
  }
}
