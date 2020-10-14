package com.mafei.steaming.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/9/30
 */
object SocketStreaming extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf: SparkConf = new SparkConf().setAppName("kkb").setMaster("local[2]")

  // todo: 2、创建StreamingContext对象
  val ssc = new StreamingContext(sparkConf,Seconds(10))

  //todo: 3、接受socket数据
  val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop03",4444)

  //todo: 4、对数据进行处理
  val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

  //todo: 5、打印结果
  result.print()

  //todo: 6、开启流式计算
  ssc.start()
  ssc.awaitTermination()
}
