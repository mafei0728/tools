package com.mafei.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/8/18
 */
object SparkSessionFactory {
  val conf: SparkConf = new SparkConf().setAppName("mafei0728").setMaster("local[2]")
  private val sparkSess: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
  val sc: SparkContext = sparkSess.sparkContext
  def getSession: SparkSession = sparkSess
}
