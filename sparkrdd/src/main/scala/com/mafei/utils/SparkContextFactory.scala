package com.mafei.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/9/17
 */
object SparkContextFactory {
  LogLevelSet.setLogLevel()
  val conf: SparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("mafei")
  val spark: SparkSession = SparkSession.builder()
    .enableHiveSupport()
    .config(conf)
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext


  def getSourcePath(file: String): String = {
    this.getClass.getClassLoader.getResource(file).getPath
  }
}
