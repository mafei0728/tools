package com.mafei.testf

import com.mafei.utils.SparkSessionFactory

/*
    @Classname GetParameter
    @Description
    @author mafei0728
    @Date 2020/8/29 15:23
    @version 1.0
*/
object GetParameter extends App {
  val spark = SparkSessionFactory.getSession
  spark.conf.set("spark.jars", "E:\\kkb\\notebook\\hadoop\\tools\\analyticfunction\\src\\main\\resources\\hadoop-lzo-0.4.20.jar")
  spark.conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec")
  spark.conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec")

  val df = spark.sql("select * from game_center.ods_user_login limit 10")
  df.show()
}
