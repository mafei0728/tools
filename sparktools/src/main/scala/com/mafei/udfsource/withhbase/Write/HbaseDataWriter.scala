package com.mafei.udfsource.withhbase.Write

import com.mafei.loadconfig.LoadConfig
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}

/*
    @Classname HbaseDataWrite
    @Description
    @author mafei0728
    @Date 2020/10/22 22:20
    @version 1.0
*/
class HbaseDataWriter extends DataWriter[Row] {
  private val conf: Config = LoadConfig.getConfig("application.properties")
  private val configuration: Configuration = HBaseConfiguration.create()
  configuration.set("hbase.zookeeper.quorum", conf.getString("zookeeper.quorum"))
  private val connection: Connection = ConnectionFactory.createConnection(configuration)
  private val table = connection.getTable(TableName.valueOf("idea"))

  override def write(record: Row): Unit = {
    val rk = record.getString(0)
    val name = record.getString(1)
    val address = record.getString(2)
    val road = record.getString(3)
    val put = new Put(rk.getBytes())
    put.addColumn("r1".getBytes(), "name".getBytes(), name.getBytes())
    put.addColumn("r2".getBytes(), "address".getBytes(), address.getBytes())
    put.addColumn("r3".getBytes(), "road".getBytes(), road.getBytes())
    table.put(put)
  }

  //  提交完毕的动作
  override def commit(): WriterCommitMessage = {
    table.close()
    connection.close()
    new HbaseWriterCommitMessage
  }

  // 异常后的动作
  override def abort(): Unit = {}
}
