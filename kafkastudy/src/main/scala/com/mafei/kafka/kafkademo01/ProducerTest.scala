package com.mafei.kafka.kafkademo01

import java.time.LocalDateTime
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/*
    @Classname ProducterTest
    @Description
    @author mafei0728
    @Date 2020/9/28 22:50
    @version 1.0
*/
object ProducerTest extends App {
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "hadoop03:9092,hadoop04:9092,hadoop05:9092")
  //消息的确认机制
  properties.put("acks", "all")
  properties.put("retries", "0")
  //缓冲区的大小  //默认32M
  properties.put("buffer.memory", "33554432")
  //批处理数据的大小，每次写入多少数据到topic   //默认16KB
  properties.put("batch.size", "16384")
  //可以延长多久发送数据   //默认为0 表示不等待 ，立即发送
  properties.put("linger.ms", "1")
  properties.put("buffer.memory", "33554432")
  //指定数据序列化和反序列化
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](properties)
  for(i <- 1 to 1000){
    // 缓慢生产数据
    Thread.sleep(2000)
    val dateGet = LocalDateTime.now().toString
    val record = new ProducerRecord[String, String]("kkb", i + "\t" + dateGet)
    producer.send(record)
  }
}
