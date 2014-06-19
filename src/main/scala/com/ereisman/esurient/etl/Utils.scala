package com.ereisman.esurient.etl


import java.sql.SQLException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path,FileSystem}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.log4j.Logger


object Utils {
  def getDfs(conf: Configuration): DistributedFileSystem = {
     val nnUri = (new Path(conf.get("fs.default.name", "ERROR_NO_FS_DEFAULT_NAME_FOUND"))).toUri
     FileSystem.get(nnUri, conf, System.getProperty("USER")).asInstanceOf[DistributedFileSystem]
  }
  
  def logException(log: Logger, exception: Throwable): Unit = {
    if (exception.isInstanceOf[SQLException]) {
      import scala.collection.JavaConversions._
      for (sqlEx <- (exception.asInstanceOf[SQLException]).iterator) {
        writeWarnLog(log, sqlEx)
      }
    } else {
      writeWarnLog(log, exception)
    }
  }

  def logFatal(log: Logger, exception: Throwable): Unit = {
    logException(log, exception)
    throw new RuntimeException(exception)
  }

  private def writeWarnLog(log: Logger, exception: Throwable): Unit = {
    log.warn("Exception: " + exception.toString)
    log.warn("Message: " + exception.getMessage)
    log.warn("Stacktrace:\n" + exception.getStackTrace.map {
      ste: StackTraceElement => ste.toString
    }.mkString("\n"))
  }
}
