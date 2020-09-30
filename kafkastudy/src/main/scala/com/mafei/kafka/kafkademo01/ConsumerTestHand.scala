package com.mafei.kafka.kafkademo01

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

import scala.collection.mutable.ListBuffer

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/9/28
 */
object ConsumerTestHand extends App {
  val properties = new Properties()
  //kafka集群地址
  properties.put("bootstrap.servers", "hadoop03:9092,hadoop04:9092,hadoop05:9092")
  //消费者组id
  properties.put("group.id", "consumer-test")
  //允许自动提交偏移量
  //properties.put("enable.auto.commit", "true")
  properties.put("enable.auto.commit", "false")
  //自动提交偏移量的时间间隔
  //properties.put("auto.commit.interval.ms", "1000")
  //默认是latest
  //earliest: 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
  //latest: 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
  //none : topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
  properties.put("auto.offset.reset", "earliest")
  properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  val consumer = new KafkaConsumer[String, String](properties)
  // 指定消费的topic
  consumer.subscribe(util.Arrays.asList("kkb"))
  // 指定打印多少消息后手动提交
  val count: Int = 10
  val statList: ListBuffer[ConsumerRecord[String, String]] = ListBuffer()
  while (true) {
    val records: ConsumerRecords[String, String] = consumer.poll(3000)
    val iterRecord = records.iterator()
    while (iterRecord.hasNext) {
      val record = iterRecord.next()
      statList.append(record)
      println(s"partition:${record.partition()}\n" +
        s"key:${record.key()}\n" +
        s"offset:${record.offset()}\n" +
        s"value:${record.value()}")
      if (statList.size >= count) {
        println(s"手动提交${statList.size}")
        statList.clear()
        try {
          consumer.commitAsync()
        } catch {
          case ex: Exception => println(ex.getStackTrace.mkString("Array(", ", ", ")"))
        } finally {
          consumer.commitSync()
        }
      }
    }
  }
}
