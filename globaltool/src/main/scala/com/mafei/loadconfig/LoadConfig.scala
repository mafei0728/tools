package com.mafei.loadconfig

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

/*
 * @Description 加载配置文件,预留外部配置的入口
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/10/18
 */
object LoadConfig {
  implicit private val fileString: String = "application.properties"

  private val fileName: String = LoadConfig.getClass.getClassLoader
    .getResource("application.properties")
    .getFile
  /*
   * @Description: 加载绝对路径
   * @param file:
   * @return com.typesafe.config.Config
   * @author: mafei0728
   * @date: 2020/10/18 23:46
   */
  def getConfig(file: File): Config = {
    Some(ConfigFactory.parseFile(file)).get
  }

  /*
   * @Description: 加载resource下面的配置文件
   * @param file:
   * @return com.typesafe.config.Config
   * @author: mafei0728
   * @date: 2020/10/18 23:46
   */
  def getConfig(file: String): Config = {
    Some(ConfigFactory.load(file)).get
  }
}
