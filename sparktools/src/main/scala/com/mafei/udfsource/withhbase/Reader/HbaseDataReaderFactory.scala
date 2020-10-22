package com.mafei.udfsource.withhbase.Reader

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}

import scala.collection.mutable

/*
    @Classname HbaseInputPartition
    @Description
    @author mafei0728
    @Date 2020/10/22 0:19
    @version 1.0
*/
class HbaseDataReaderFactory(val read: String, val hbaseSchema: mutable.HashMap[String,String]) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = {
    new HbaseDataReader(read,hbaseSchema)
  }
}
