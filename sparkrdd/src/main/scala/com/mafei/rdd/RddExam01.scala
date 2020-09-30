package com.mafei.rdd

import com.mafei.utils.{LogLevelSet, SparkContextFactory}
import org.apache.spark.rdd.RDD

/*
    @Classname rddtest
    @Description
    @author mafei0728
    @Date 2020/9/16 20:58
    @version 1.0
*/
object RddExam01 extends App {
  val sc = SparkContextFactory.sc
  LogLevelSet.setLogLevel()
  // 读取文件
  val rdd01: RDD[String] = sc.textFile("src/main/resources/access.log")
  // 过滤出含有网站的数据
  val rdd02 = rdd01.filter(_.contains("http"))
  // 取除http
  val rdd03 = rdd02.map(_.split(" ")(10)).filter(_.contains("http"))
  // 求每个url
  val t = rdd03.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false).take(5)
  // 逆序打印排行前5的url
  t.foreach(println)

}
