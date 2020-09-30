package com.mafei.udfs.udf

import com.mafei.utils.{LogLevelSet, SparkSessionFactory}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession, functions => f}

import scala.util.matching.Regex

/*
    @Classname Example01  dataframe 和 sql的自定义函数有区别
    @Description
    @author mafei0728
    @Date 2020/9/23 22:56
    @version 1.0
*/
object Example01 {
  LogLevelSet.setTestLevel()
  lazy val spark: SparkSession = SparkSessionFactory.getSession
  spark.catalog.setCurrentDatabase("game_center")
  val numPattern: Regex = "([0-9]+)".r


  def reg(x:String):String={
    val res = numPattern.findFirstIn(x)
    res match {
      case Some(x) => x
      case None => "1024"
    }
  }

  def fun1(): Unit = {
    // 函数名,传入类型.返回类型
    spark.udf.register("udf01", new UDF1[String, String] {
      override def call(t1: String): String = {
        reg(t1)
      }
    }, DataTypes.StringType) // 返回类型
    val df: DataFrame = spark.sql("select udf01(attacker_rname) from ods_death_log limit 10")
    df.show()
  }

  def fun02(): Unit = {
    val lam = spark.udf.register("lam", (x: String) => {
        reg(x)
    })
    var df:DataFrame = spark.sql("select attacker_rname from ods_death_log limit 10")
    df = df.withColumn("attacker_rname", lam(f.col("attacker_rname")))
    df.show()
  }

  def main(args: Array[String]): Unit = {
      fun02()
  }


}
