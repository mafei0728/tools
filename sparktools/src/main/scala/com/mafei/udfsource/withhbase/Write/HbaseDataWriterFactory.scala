package com.mafei.udfsource.withhbase.Write

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}

/*
    @Classname HbaseWriterFactory
    @Description
    @author mafei0728
    @Date 2020/10/22 22:19
    @version 1.0
*/
class HbaseDataWriterFactory extends DataWriterFactory[Row]{
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new HbaseDataWriter
  }
}
