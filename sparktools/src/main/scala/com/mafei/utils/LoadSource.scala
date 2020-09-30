package com.mafei.utils

import java.io.InputStream
import java.util.Properties

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/9/24
 */
object LoadSource {
  def getSourcePath(source:String): String ={
    this.getClass.getClassLoader.getResource(source).getPath
  }

  def getSourceInPutStream(source:String):InputStream={
    this.getClass.getClassLoader.getResourceAsStream(source)
  }

  def getProperties(source:String):Properties={
    val properties = new Properties()
    properties.load(getSourceInPutStream(source))
    properties
  }

  def main(args: Array[String]): Unit = {
    println(getSourcePath("mysql.properties"))
  }
}
