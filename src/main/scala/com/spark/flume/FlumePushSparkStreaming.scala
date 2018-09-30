package com.spark.flume


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume._
import org.apache.spark.streaming.flume.sink.SparkSink

/**
  * Created by Administrator on 2018/8/19.
  */
object FlumePushSparkStreaming {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("flume-push").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //推送方式： flume向spark 发送数据
    val flumeStream = FlumeUtils.createStream(ssc,"192.168.2.1",4141)
    //flume中的数据通过event.getBody()/才能拿到真正的内容
    val words = flumeStream.flatMap(x => new String(x.event.getBody.array).split(" ")).map((_,1))
    val results = words.reduceByKey(_+_)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
