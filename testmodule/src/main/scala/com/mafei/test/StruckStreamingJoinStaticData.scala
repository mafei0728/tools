package com.mafei.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions => f}

/*
 * @Description spark结构化流与静态数据的交互
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/10/12
 */
object StruckStreamingJoinStaticData extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("demo01").setMaster("local[2]")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._
  case class UserInfo(id: Int, firstName: String)
  val staticDate = List((1, "zhao"), (2, "qian"), (3, "sun"), (4, "li"))
  var bf = spark.createDataFrame(staticDate.map(x => UserInfo(x._1, x._2))).as[UserInfo]
  // 广播静态ds
  bf = f.broadcast(bf)
  // 读取socket里面的数据  1,li 2,wo
  val line = spark.readStream.format("socket")
    .option("host", "hadoop03")
    .option("port", 4567)
    .load()
  var staticRes = line.as[String].withColumn("id", f.split(f.col("value"),",")(0))
    .withColumn("lastName", f.split(f.col("value"),",")(1)).drop("value")

  val res = staticRes.join(bf, Seq("id"), "inner").select(
    staticRes.col("*"),bf.col("firstName"))

  val query = res.writeStream.outputMode("append").format("console").start()
  query.awaitTermination()
}
