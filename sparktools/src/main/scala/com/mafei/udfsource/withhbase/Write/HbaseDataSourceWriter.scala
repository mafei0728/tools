package com.mafei.udfsource.withhbase.Write

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}

/*
    @Classname HbaseDataSourceOption
    @Description
    @author mafei0728
    @Date 2020/10/22 22:13
    @version 1.0
*/
class HbaseDataSourceWriter extends DataSourceWriter{
  override def createWriterFactory(): DataWriterFactory[Row] = {
    new HbaseDataWriterFactory
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}
