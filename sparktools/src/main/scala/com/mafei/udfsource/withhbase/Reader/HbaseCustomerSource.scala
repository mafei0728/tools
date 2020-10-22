package com.mafei.udfsource.withhbase.Reader

import java.util.Optional

import com.mafei.udfsource.withhbase.Write.HbaseDataSourceWriter
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
/*
    @Classname HbaseCustomerSource
    @Description
    @author mafei0728
    @Date 2020/10/21 23:12
    @version 1.0
*/
class HbaseCustomerSource extends DataSourceV2 with ReadSupport with WriteSupport{
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val read = options.get("hbase.name.read").get()
    val hbaseSchema = getHbaseSchema(options.get("hbase.table.schema").get())
    val sparkSchema = options.get("spark.data.schema").get()
    new HbaseCustomerSourceReader(read, hbaseSchema, sparkSchema)
  }

  def getHbaseSchema(value:String):mutable.HashMap[String,String]={
    val mapResult:mutable.HashMap[String,String] = mutable.HashMap()
    value.split(",").foreach(x=>{
      mapResult(x.split(":")(0)) = x.split(":")(1)
    })
    mapResult
  }

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions):
  Optional[DataSourceWriter] = {
    Optional.of(new HbaseDataSourceWriter)
  }
}
