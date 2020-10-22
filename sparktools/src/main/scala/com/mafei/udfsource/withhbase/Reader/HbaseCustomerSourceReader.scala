package com.mafei.udfsource.withhbase.Reader

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/*
    @Classname HbaseCustomerSourceReader
    @Description 自定义hbase数据源,谓词下推和 col 剪裁
    @author mafei0728
    @Date 2020/10/21 22:50
    @version 1.0
*/
class HbaseCustomerSourceReader(val read: String, val hbaseSchema: mutable.HashMap[String,String], val sparkSchema: String) extends DataSourceReader {
  override def readSchema(): StructType = {
    StructType.fromDDL(sparkSchema)
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    import scala.collection.JavaConverters._
    Seq(new HbaseDataReaderFactory(read, hbaseSchema).asInstanceOf[DataReaderFactory[Row]]).asJava
  }
}
