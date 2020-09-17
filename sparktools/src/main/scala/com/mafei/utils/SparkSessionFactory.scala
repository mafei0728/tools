package com.mafei.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/8/18
 */
object SparkSessionFactory {
  Logger.getLogger("org").setLevel(Level.WARN)
  private val conf: SparkConf = new SparkConf()
  conf.setAppName("mafei0728").setMaster("local[2]")
  private val sparkSess: SparkSession = SparkSession.builder()
    .enableHiveSupport()
    .config(conf)
    .getOrCreate()
  val sc: SparkContext = sparkSess.sparkContext

  def getSession: SparkSession = {
    sparkSess
  }

  /**
   * create by: mafei0728
   * description: TODO
   * create time: 2020/8/18 22:30
   *
   * @ Param: df
   * @ Param: table_name
   * @ Param: partition
   * @ return void
   */
  def DFSaveToHive(df: DataFrame, table_name: String, partition_cols: String, partition: Int = 1): Unit = {
    df.write.mode("overwrite").partitionBy()
    sparkSess.catalog.tableExists("sss")
  }
}
