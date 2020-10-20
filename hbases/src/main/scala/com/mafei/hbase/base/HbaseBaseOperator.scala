package com.mafei.hbase.base

import java.io.File
import java.util

import com.mafei.loadconfig.LoadConfig
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Get, Put, Result, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/*
    @Classname HbaseBaseOperator
    @Description
    @author mafei0728
    @Date 2020/10/18 22:35
    @version 1.0
*/
class HbaseBaseOperator {

}

object HbaseBaseOperator {
  // 1. 创建配置文件
  val conf: Config = LoadConfig.getConfig("application.properties")
  val configuration: Configuration = HBaseConfiguration.create()
  configuration.set("hbase.zookeeper.quorum", conf.getString("zookeeper.quorum"))
  // 创建对象连接
  val connection: Connection = ConnectionFactory.createConnection(configuration)
  val admin: Admin = connection.getAdmin
  val table: Table = connection.getTable(TableName.valueOf("ns01", "idea"))

  // 创建一张表
  def createTable(): Unit = {
    // 没有namespace则为默认
    val namespace = "ns01"
    val tableName = "idea"
    val tb = TableName.valueOf(namespace, tableName)
    // 判断命令空间是否存在
    createNameSpace(namespace)
    // 创建表
    createTable(tableName, "r1", "r2", "r3")
  }

  //创建命令空间的方法
  def createNameSpace(name: String): Unit = {
    try {
      // 如果不存在就报异常
      admin.getNamespaceDescriptor(name)
    } catch {
      case ex: Exception =>
        val namespace = NamespaceDescriptor.create(name).build()
        admin.createNamespace(namespace)
    }
  }

  // 创建表
  def createTable(tableName: String, columnsFamily: String*): Unit = {
    val tb = TableName.valueOf(tableName)
    if (!admin.tableExists(tb)) {
      val tbs = new HTableDescriptor(tb)
      for (col <- columnsFamily) {
        tbs.addFamily(new HColumnDescriptor(col))
      }
      admin.createTable(tbs)
    } else {
      println("表已经存在")
    }
  }

  // 添加一列数据
  def addData(): Unit = {
    val table = connection.getTable(TableName.valueOf("ns01", "idea"))
    // 创建put对象指向rowkey
    val put: Put = new Put("rk001".getBytes())
    put.addColumn("r1".getBytes(), "name".getBytes(), "ma".getBytes())
    put.addColumn("r2".getBytes(), "address".getBytes(), "xt".getBytes())
    put.addColumn("r3".getBytes(), "road".getBytes(), "zg".getBytes())
    table.put(put)
    // 记得关闭table
    table.close()
  }

  //  添加多列数据

  def addDatas(): Unit = {
    val table = connection.getTable(TableName.valueOf("ns01", "idea"))
    val putArray: util.ArrayList[Put] = new util.ArrayList[Put]()
    val put01: Put = new Put("rk002".getBytes())
    put01.addColumn("r1".getBytes(), "name".getBytes(), "zhu".getBytes())
    put01.addColumn("r2".getBytes(), "address".getBytes(), "xtt".getBytes())
    put01.addColumn("r3".getBytes(), "road".getBytes(), "zg".getBytes())
    val put02: Put = new Put("rk003".getBytes())
    put02.addColumn("r1".getBytes(), "name".getBytes(), "liu".getBytes())
    put02.addColumn("r2".getBytes(), "address".getBytes(), "xttt".getBytes())
    put02.addColumn("r3".getBytes(), "road".getBytes(), "zg".getBytes())
    putArray.add(put01)
    putArray.add(put02)
    table.put(putArray)
    table.close()
  }

  // 查询一条数据
  def getData(): Unit = {
    val get: Get = new Get("rk001".getBytes())
    val res = table.get(get)
    val cells: util.List[Cell] = res.listCells()
    // scala 操作需要隐式转换一下
    import scala.collection.JavaConversions.asScalaBuffer
    for (cell <- cells) {
      println(s"${Bytes.toString(CellUtil.cloneFamily(cell))}")
      println(s"${Bytes.toString(CellUtil.cloneQualifier(cell))}")
      println(s"${Bytes.toString(CellUtil.cloneRow(cell))}")
      println(s"${Bytes.toString(CellUtil.cloneValue(cell))}")
      println("--------------------------")

    }
  }

  // 读取hbase转化成df
  def hbaseToDF(): Unit = {
    configuration.set(TableInputFormat.INPUT_TABLE, "ns01:idea")
    val conf = new SparkConf().setAppName("hbase").setMaster("local[1]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val hbase = sc.newAPIHadoopRDD(configuration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    hbase.foreach(println)
  }


  def main(args: Array[String]): Unit = {
    HbaseBaseOperator.hbaseToDF()
  }
}
