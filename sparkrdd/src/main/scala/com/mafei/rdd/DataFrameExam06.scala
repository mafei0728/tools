package com.mafei.rdd

import com.mafei.utils.SparkContextFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mafei.utils.LogLevelSet

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0m
 * @Date 2020/9/22
 */
object DataFrameExam06 extends App{
  LogLevelSet.setLogLevel()
  val spark: SparkSession = SparkContextFactory.spark
  spark.catalog.setCurrentDatabase("game_center")
  val df: DataFrame = spark.sql("select * from ods_death_log limit 10")
  df.show()
}
