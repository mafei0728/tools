package com.mafei.utils

import org.apache.log4j.{Level, Logger}

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/9/17
 */
object LogLevelSet {
  def setLogLevel(): Unit = Logger.getLogger("org").setLevel(Level.ERROR)

}
