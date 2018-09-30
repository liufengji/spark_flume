package com.spark.flume


import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

/**
  * Created by Administrator on 2018/8/19.
  */
object FlumePullSparkStreaming {
  def main(args: Array[String]): Unit = {
    //官网注意事项
    //http://spark.apache.org/docs/latest/streaming-flume-integration.html

    //在flume /lib/ 目录下添加
    //spark-streaming-flume-sink_2.11-2.3.1.jar
    //scala-library-2.11.8.jar
    //commons-lang3-3.5.jar
    val sparkConf = new SparkConf().setAppName("flume-push").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //拉方式
    val address = Seq(new InetSocketAddress("hadoop103",4141))
    val flumeStream = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK)
    //flume中的数据通过event.getBody()/才能拿到真正的内容
    val words = flumeStream.flatMap(x => new String(x.event.getBody.array).split(" ")).map((_, 1))
    val results = words.reduceByKey(_ + _)
    results.print()
    ssc.start()
    ssc.awaitTermination()
    //问题
    //spark streaming 使用flume pull 遇到begin() called when transaction is OPEN!
    //原因是因为flume lib下的scala的版本问题，建议使用scala-library-2.11.x.jar
    //rm -rf scala-library-2.10.x.jar
    //把$FLUME_HOME/bin文件夹下面的flume-ng里面的内容修改为 JAVA_OPTS="-Xmx2048m"
  }
}

