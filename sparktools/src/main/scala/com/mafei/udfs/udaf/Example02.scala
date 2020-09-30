package com.mafei.udfs.udaf

import java.util

import com.mafei.utils.{LogLevelSet, SparkSessionFactory}
import org.apache.spark.sql.{Row, SparkSession, functions => f}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StringType, DataType, IntegerType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/9/29
 */

/*
 * @Description: 求开销中位数
 * @param null:
 * @return
 * @author: mafei0728
 * @date: 2020/9/29 22:54
 */
class Example02 extends UserDefinedAggregateFunction {
  val splitStr = "#"

  // 聚合的数据类型
  override def inputSchema: StructType = {
    StructType(StructField("consume_count", IntegerType) :: Nil)
  }

  // 缓冲区数据类型
  override def bufferSchema: StructType = {
    StructType(StructField("count_array", StringType) :: Nil)
  }

  // 返回的数据类型
  override def dataType: DataType = IntegerType

  // 对于相同的输入是否一直返回相同的输出。
  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getString(0) + splitStr + input.getInt(0).toString)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getString(0) + buffer2.getString(0)
  }

  override def evaluate(buffer: Row): Int = {
    val resArray = buffer.getString(0).split(splitStr).sorted
    resArray.length % 2 match {
      case 1 => resArray(resArray.length / 2).toInt
      case 0 => (resArray(resArray.length / 2).toInt + resArray((resArray.length / 2) - 1).toInt) / 2
    }
  }
}

object Example02 {
  LogLevelSet.setTestLevel()
  lazy val spark: SparkSession = SparkSessionFactory.getSession
  spark.catalog.setCurrentDatabase("game_center")


  def main(args: Array[String]): Unit = {
    val df = spark.sql("select server_id,consume_count from ods_monetary_consume")
    val medianF = spark.udf.register("medianF", new Example02)
    df.groupBy("server_id").agg(medianF(f.col("consume_count")).alias("t")).show(10)
  }

}
