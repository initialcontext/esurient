package com.ereisman.esurient.etl.format


import java.io.OutputStream
import java.sql.ResultSet


/**
 * Pluggable interface to output each record from a ResultSet for output to HDFS.
 */
trait EtlOutputFormatter {

  def formatRecord(resultSet: ResultSet, toHdfsFile: OutputStream): Unit = {  }

}
