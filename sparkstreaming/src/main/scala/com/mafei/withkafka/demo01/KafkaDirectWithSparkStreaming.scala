package com.mafei.withkafka.demo01

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
 * @Description
 * @Author mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/10/9
 */
object KafkaDirectWithSparkStreaming extends App{
  // 修改日志级别
  Logger.getLogger("org").setLevel(Level.ERROR)
  // 创建配置文件对象
  val sparkConf: SparkConf = new SparkConf().setAppName("kkb").setMaster("local[2]")
  // todo: 2、创建StreamingContext对象,周期为10秒
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  // topic
  val topic: Set[String] = Set("bigdata")
  // kafka参数
  val kafkaParams = Map(
    "bootstrap.servers" -> "hadoop03:9092,hadoop04:9092,hadoop05:9092",
    "group.id" -> "KafkaDirect10",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "enable.auto.commit" -> "false" // 关闭手动提交
  )
  // 获得 kafkaDStream
  val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String,String](topic,kafkaParams)
  )
  // 处理数据
  kafkaDStream.foreachRDD(rdd => {
    val resRdd:RDD[String] = rdd.map(_.value())
    resRdd.foreach(println)
    // 手动提交偏移量
    val offsetRange: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRange)
  })
  ssc.start()
  ssc.awaitTermination()
}
