package com.mafei.streamingdemo03

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._

/*
    @Classname StruckStreamingWithKafka
    @Description: 读取kafka数据写入mysql
    @author mafei0728
    @Date 2020/10/13 21:18
    @version 1.0
*/
object StruckStreamingWithKafka extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  //创建SparkSession
  val spark = SparkSession
    .builder()
    .appName("struckwithkafka").master("local[*]")
    .getOrCreate()

  import spark.implicits._

  //读取kafka内容
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "hadoop03:9092,hadoop04:9092,hadoop05:9092")
    .option("subscribe", "struckwithkafka")
    .option("startingOffsets", "latest") //注意流式处理没有endingOffset
    .option("includeTimestamp", value = true) //输出内容包括时间戳
    .load()

  case class UserInfo(id: Int, name: String, province: String, city: String, time: Timestamp)

//   1,zhao,湖北,武汉,timestamp
  val dataset: Dataset[(String, Timestamp)] = df.select("value", "timestamp")
    .withColumn("value", df.col("value").cast(StringType))
    .withColumn("timestamp", df.col("timestamp").cast(TimestampType))
    .as[(String, Timestamp)]
  val res = dataset.map(line => {
    val lineArray = line._1.split(",")
    UserInfo(lineArray(0).toInt, lineArray(1), lineArray(2), lineArray(3), line._2)
  }).as[UserInfo]
  val qes = df.writeStream.queryName("kafka_test")
    .outputMode(OutputMode.Append())
    .format("console")
    .start()
  qes.awaitTermination()
}
