package com.mafei.DemoTest

import com.mafei.utils.SparkSessionFactory
import com.mafei.utils.LoadSource
import com.mafei.utils.LogLevelSet

/*
    @Classname ReadFromcsvToMsql
    @Description
    @author mafei0728
    @Date 2020/9/24 20:46
    @version 1.0
*/
object ReadFromcsvToMsql extends App {
  LogLevelSet.setTestLevel()
  val spark = SparkSessionFactory.getSession
  val df = spark.read.csv(LoadSource.getSourcePath("test.csv"))
  val properties = LoadSource.getProperties("mysql.properties")
  df.write.mode("overwrite").jdbc(properties.getProperty("url"), "kkb_csv", properties)
}
