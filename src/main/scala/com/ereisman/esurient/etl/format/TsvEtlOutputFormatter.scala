package com.ereisman.esurient.etl.format


import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration

import com.ereisman.esurient.EsurientConstants._

import java.sql.ResultSet
import java.io.OutputStream


object TsvEtlOutputFormatter {
  val BAD_SHARD_ID_COLUMN = "\tERROR_BAD_SHARD_ID_VALUE"
  val LOG = Logger.getLogger(classOf[TsvEtlOutputFormatter])
  val ENDL: Array[Byte] = System.getProperty("line.separator").getBytes("UTF-8")
}

/**
 * Pluggable interface to output each record from a ResultSet for output to HDFS.
 */
class TsvEtlOutputFormatter(val conf: Configuration) extends EtlOutputFormatter {
  import com.ereisman.esurient.etl.format.TsvEtlOutputFormatter._
  var colCount = ES_ERROR_CODE
  val taskId = conf.getInt(ES_THIS_TASK_ID, ES_ERROR_CODE)
  val suffix = getShardIdSuffix(conf)


  override def formatRecord(resultSet: ResultSet, hdfsStream: OutputStream): Long = {
    val buffer = getRowAsUtfBytes(resultSet)
    hdfsStream.write(buffer, 0, buffer.length)
    hdfsStream.write(ENDL, 0, ENDL.length)

    (buffer.length + ENDL.length).toLong
  }


  private def getRowAsUtfBytes(resultSet: ResultSet): Array[Byte] = {
    var cols = getColCount(resultSet)
    val tsv: String = (1 to cols).map { index: Int =>
      resultSet.getObject(index) match {
        case null                   => "NULL"
        case obj: java.lang.Object  => obj.toString match {
          case str: String =>
          str.replaceAll("\r", """\\r""").replaceAll("\n", """\\n""").replaceAll("\t", """\\t""")
          case _          => LOG.warn("Bad data record encountered at column " + index) ; "NULL"
        }
      }
    }.toList.mkString("\t")

    (tsv + suffix).getBytes("UTF-8")
  }


  // appended shard id if this is a sharded table for proper deduping in post-processing
  private def getShardIdSuffix(conf: Configuration): String = {
    conf.getBoolean(ES_DB_SHARDED_TABLE, false) match {
      // assume (task id -> shard id) mapping if this is a sharded table
      case true     => "\t" + taskId
      case false    => ""
    }
  }


  private def getColCount(resultSet: ResultSet): Int = {
    colCount match {
      case i: Int if (i > ES_ERROR_CODE) => i
      case _      => colCount = resultSet.getMetaData.getColumnCount ; colCount
    }
  }
}
