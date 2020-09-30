package com.mafei.rdd

import com.mafei.utils.LogLevelSet
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/9/21
 */
case class Person(id: Int, name: String, age: Int)

object Test extends App{
  LogLevelSet.setLogLevel()
  val spark: SparkSession = SparkSession.builder().appName("kkb").master("local[2]").getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val path: String = this.getClass.getClassLoader.getResource("person.txt").getPath
  val rdd = sc.textFile(path)
    .map(_.split(" "))
    .map(x=>Person(x(0).toInt,x(1),x(2).toInt))
  import spark.implicits._
  val dfPerson:DataFrame = rdd.toDF()
  dfPerson.show()

}
