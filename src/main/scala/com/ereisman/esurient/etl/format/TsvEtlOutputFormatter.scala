package com.ereisman.esurient.etl.format


import org.apache.hadoop.conf.Configuration

import com.ereisman.esurient.EsurientConstants._

import java.sql.ResultSet


/**
 * Pluggable interface to output each record from a ResultSet for output to HDFS.
 */
class TsvEtlOutputFormatter(val conf: Configuration) extends EtlOutputFormatter {
  var colCount = ES_ERROR_CODE
  val suffix = getShardIdSuffix(conf)


  override def formatRecord(resultSet: ResultSet): String = {
    var cols = getColCount(resultSet)
    val tsv = (1 to cols).map { index: Int =>
      resultSet.getObject(index).toString match {
        case null | "null" | "NULL" => "NULL"
        case str: String            =>
          str.replaceAll("\r", "\\r").replaceAll("\n", "\\n").replaceAll("\t", "\\t")
      }
    }.mkString("\t")

    tsv + suffix + "\n"
  }

  // appended shard id if this is a sharded table for proper deduping in post-processing
  private def getShardIdSuffix(conf: Configuration): String = {
    conf.getBoolean(ES_DB_SHARDED_TABLE, false) match {
      // assume (task id -> shard id) mapping if this is a sharded table
      case true => "\t" + conf.get(ES_THIS_TASK_ID, "ERROR_NO_SHARD_ID_FOUND")
      case _    => ""
    }
  }

  private def getColCount(resultSet: ResultSet): Int = {
    colCount match {
      case i: Int if (i > ES_ERROR_CODE) => i
      case _      => colCount = resultSet.getMetaData.getColumnCount ; colCount
    }
  }
}
