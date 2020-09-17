package com.mafei.utils

import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/9/17
 */
object SparkContextFactory {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mafei")
  val sc: SparkContext = new SparkContext(conf)
}
