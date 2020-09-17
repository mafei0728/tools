package com.mafei.anf

import com.mafei.utils.SparkSessionFactory
import org.apache.spark.sql.{DataFrame, SparkSession,functions=>f}

object AnalyticFunction {
  def main(args: Array[String]): Unit = {
    fun02()
  }

  val spark: SparkSession = SparkSessionFactory.getSession
  val dataL: Seq[(String, Int, String, Int)] = Seq(
    ("A", 1, "12:10:13", 2),
    ("A", 1, "12:11:13", 2),
    ("A", 1, "12:12:13", 2),
    ("B", 1, "12:13:13", 2),
    ("A", 1, "12:12:13", 1),
    ("B", 3, "12:14:13", 2),
    ("B", 3, "12:15:13", 3),
    ("B", 3, "12:16:13", 3),
    ("B", 7, "12:17:13", 2),
    ("A", 4, "12:18:13", 2),
    ("A", 1, "12:19:13", 3),
    ("B", 7, "12:19:33", 2),
    ("B", 7, "12:17:53", 3))

  case class Basket(team: String, number: Int, time: String, score: Int)

  var df: DataFrame = spark.createDataFrame(dataL.map(a => Basket(a._1, a._2, a._3, a._4)))
  df = df.cache()

  // 行转列
  def fun01(): DataFrame = {
    df = df.groupBy("time", "number").pivot("team").sum("score")
    df.show()
    df
  }

  // 列传行
  def fun02(): Unit = {
    df.withColumn("time", f.explode(f.split(f.col("time"),":"))).show()
  }
}
