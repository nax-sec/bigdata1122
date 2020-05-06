package com.atguigu.sparkstream.day04.transformations

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object Window {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop103", 9999)

    //直接使用window
    //    socket.window(Seconds(9),Seconds(6)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print

    //使用window的其他状态转换操作  仅对某一个操作开窗
    //    socket.flatMap(_.split(" ")).map((_,1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(9),Seconds(6)).print

    socket.flatMap(_.split(" ")).map((_,1)).reduceByKeyAndWindow(
      (a:Int,b:Int)=>a+b,
      (a:Int,b:Int)=>a-b,
      Seconds(9),
      Seconds(6),
      2,
      (a:(String,Int))=>a._2>0
    ).print

    ssc.checkpoint("D:\\Mywork\\bigdata1122\\sparkstream\\src\\ck")

    ssc.start

    ssc.awaitTermination

  }
}
