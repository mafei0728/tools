package com.mafei.rdd

import com.mafei.utils.SparkContextFactory
/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/9/22
 */
object DataFrameExam05 extends App {
  val spark = SparkContextFactory.spark
  val df = spark.read.json(SparkContextFactory.getSourcePath("person.json"))
  df.show()
}
