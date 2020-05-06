package com.atguigu.sparkstream.day04.transformations

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    //    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    //    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //    
    //    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop103", 9999)
    //
    //    val ds: DStream[(String, Int)] = socket.flatMap(_.split(" ")).map((_, 1))
    //
    //    ds.updateStateByKey[Int]((a:Seq[Int],b:Option[Int])=>Some(a.sum+b.getOrElse(0))).print
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("D:\\Mywork\\bigdata1122\\sparkstream\\src\\ck", getssc)
    ssc.checkpoint("D:\\Mywork\\bigdata1122\\sparkstream\\src\\ck")
    ssc.start
    ssc.awaitTermination
  }
  def getssc() ={
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop103", 9999)

    val ds: DStream[(String, Int)] = socket.flatMap(_.split(" ")).map((_, 1))

    ds.updateStateByKey[Int]((a:Seq[Int],b:Option[Int])=>Some(a.sum+b.getOrElse(0))).print

    ssc
  }
}

