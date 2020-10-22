package com.mafei.utils

import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.log4j.Level
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
  // 创建spark conf
  val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local[1]")

  val sparkSess: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = sparkSess.sparkContext
  def getSession: SparkSession = sparkSess

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionFactory.getSession
    spark.createDataFrame(Seq((1,2),(2,3))).toDF("a","b").show()
  }
}
