package com.mafei.udfsource.withhbase.Reader

import com.mafei.utils.{LoadConfig, LogLevelSet, SparkSessionFactory}
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/*
 * @Description 读写->hbase 处理->spark
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/10/21
 */
object HbaseDemo {
  // 设置日志级别
  LogLevelSet.setTestLevel(Level.WARN)
  lazy private val spark: SparkSession = SparkSessionFactory.getSession
  lazy private val confLoad = LoadConfig.getConfig("application.properties")

  /*
   * @Description:
   * @param :
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @author: mafei0728
   * @date: 2020/10/22 22:46
   */
  def getDataFrameFromHbase: DataFrame = {
    val df: DataFrame = spark.read.format("com.mafei.udfsource.withhbase.Reader.HbaseCustomerSource")
      .option("hbase.name.read", confLoad.getString("hbase.name.read"))
      .option("hbase.table.schema", confLoad.getString("hbase.table.schema"))
      .option("spark.data.schema", confLoad.getString("spark.data.schema"))
      .load()
    df
  }

  def writeToHbaseFromDataFrame(df: DataFrame): Unit = {
      df.write.format("com.mafei.udfsource.withhbase.Reader.HbaseCustomerSource")
        .mode(SaveMode.Overwrite)
        .save()
  }

  def main(args: Array[String]): Unit = {
    println("开始从hbase读取")
    val df = getDataFrameFromHbase
    df.show()
    println("开始写入hbase")
    writeToHbaseFromDataFrame(df)
    println("写入完毕!!!")
  }
}
