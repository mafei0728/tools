package com.mafei.udfsource.withhbase.Reader

import com.mafei.loadconfig.LoadConfig
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReader

import scala.collection.mutable

/*
    @Classname HbaseInputPartitionReader
    @Description
    @author mafei0728
    @Date 2020/10/22 0:21
    @version 1.0
*/
class HbaseDataReader(val read: String, val hbaseSchema: mutable.HashMap[String, String]) extends DataReader[Row] {
  // 计算出迭代器
  private val conf: Config = LoadConfig.getConfig("application.properties")
  private val configuration: Configuration = HBaseConfiguration.create()
  configuration.set("hbase.zookeeper.quorum", conf.getString("zookeeper.quorum"))
  private val connection: Connection = ConnectionFactory.createConnection(configuration)
  private val iter = getIterator

  def getIterator: Iterator[Seq[AnyRef]] = {
    import scala.collection.JavaConverters._
    val table: Table = connection.getTable(TableName.valueOf(read.split(":")(0), read.split(":")(1)))
    val resultScanner: ResultScanner = table.getScanner(new Scan())
    resultScanner.iterator().asScala.map(eachResult => {
      val rk: String = Bytes.toString(eachResult.getRow)
      val name: String = Bytes.toString(eachResult.getValue("r1".getBytes(), "name".getBytes()))
      val address: String = Bytes.toString(eachResult.getValue("r2".getBytes(), "address".getBytes()))
      val road: String = Bytes.toString(eachResult.getValue("r3".getBytes(), "road".getBytes()))
      Seq(rk, name, address, road)
    })

  }

  override def next(): Boolean = {
    iter.hasNext
  }

  override def get(): Row = {
    val seq = iter.next()
    Row.fromSeq(seq)
  }

  override def close(): Unit = {
    connection.close()
  }
}
