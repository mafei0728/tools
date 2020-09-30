package com.mafei.utils

import org.apache.log4j.Level._
import org.apache.log4j.{Level, Logger}

/*
    @Classname LogLevelSet
    @Description
    @author mafei0728
    @Date 2020/9/20 21:49
    @version 1.0
*/
object LogLevelSet {
  def setTestLevel(level: Level = WARN): Unit = {
    Logger.getLogger("org").setLevel(level)
  }
}
