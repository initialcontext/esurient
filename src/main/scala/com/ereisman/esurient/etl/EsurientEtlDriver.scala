package com.ereisman.esurient.etl


import java.sql.ResultSet
import java.sql.SQLException
import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path,FSDataOutputStream}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.log4j.Logger

import com.ereisman.esurient.etl.db.{Database,DatabaseFactory}
import com.ereisman.esurient.EsurientTask
import com.ereisman.esurient.EsurientConstants._
import com.ereisman.esurient.etl.format.EtlOutputFormatter


object EsurientEtlDriver {
  val LOG = Logger.getLogger(classOf[EsurientEtlDriver])
}


/**
 * Called from the EsurientEtlTask#execute method, this drives
 * the ETL job, manages the lifecycle of the DB connections
 * and related references, and handles retries for various
 * failure states.
 *
 * It is assumed that an earlier run of EsurientEtlMetadataManager
 * has produced a Java Properties file on HDFS which this driver
 * will use to configure the snapshot job, DB connections, etc.
 */
class EsurientEtlDriver(val conf: Configuration, val outputFormatter: EtlOutputFormatter) {
  import com.ereisman.esurient.etl.EsurientEtlDriver.LOG
  
  // The contents of these fields are subject to reinitialization during retries,
  // and their lifecycles are managed by this task. Therefore, they are mutable.
  var db: Database = null
  var rs: ResultSet = null
  var dfs: DistributedFileSystem = null
  var stream: FSDataOutputStream = null
  var retries: Int = ES_ERROR_CODE
  var formatter: EtlOutputFormatter = null
  var outPath: Path = null

  ///// EXECUTE THE JOB /////
  retries = conf.getInt(ES_DB_RETRIES, ES_DB_RETRIES_DEFAULT)

  try {
    while (retries > 0) {
      try {
        performSnapshot(conf)
      } catch {
        // we can attempt some retries under SQL or Hadoop I/O problems
        case ex @ (_: SQLException | _: IOException) => {
          retryExceptionHandler(LOG, ex)
          retries -= 1
          LOG.warn(retries + " retry attempts remaining...")
        }
        // if its a RuntimeException or similar, let it blow up
        case ex: Throwable => blowUp(LOG, ex)
      }
    }
  } finally {
    closeResources
  }
  ///// CONSTRUCTOR ENDS HERE /////



  // (re)initialize class state for this snap attempt, execute the snapshot
  private def performSnapshot(conf: Configuration): Unit = {
    // Hadoop I/O objects
    dfs = Utils.getDfs(conf)
    outPath = getOutputPath(conf)
    stream = dfs.create(outPath, true)

    // database set up and query submission
    db = DatabaseFactory.getDatabase(conf)
    rs = submitQuery(conf)
    val shardId = conf.getInt(ES_THIS_TASK_ID, ES_ERROR_CODE).toString 

    // parse & clean each record, then write to HDFS
    do {
      while (rs.next) {
        stream.writeUTF(outputFormatter.formatRecord(rs))
      }
    } while (moreResults)
  }


  private def moreResults: Boolean = {
    LOG.info("Attempting to fetch next ResultSet...")
    rs.getStatement.getMoreResults match {
      case true => rs = rs.getStatement.getResultSet ; true
      case _    => false
    }
  }


  private def submitQuery(conf: Configuration): ResultSet = { 
    conf.get(ES_DB_MODE, "ERROR_NO_MODE_SUPPLIED") match {
      case ES_DB_BOOTSTRAP_MODE => db.fullTableSnapshot.get
      case ES_DB_UPDATE_MODE    => db.updateTableSnapshot.get
      case _                    =>
        throw new RuntimeException("No job mode (bootstrap or update) supplied in Configuration, aborting.")
    }
  }


  private def retryExceptionHandler(log: Logger, ex: Throwable): Unit = {
    Utils.logException(log, ex)
    if (stream != null) { stream.hflush ; stream.close }
    closeResources
    Thread.sleep(ES_DB_RETRY_SLEEP_MILLIS)
  }


  private def blowUp(log: Logger, ex: Throwable): Unit = {
    Utils.logException(LOG, ex)
    cleanupPartialOutput
    throw new RuntimeException(ex)
  }


  private def cleanupPartialOutput: Unit = {
    if (stream != null) { stream.close }
    if (dfs != null) {
      dfs.exists(outPath) match {
        case true => dfs.deleteOnExit(outPath)
        case _    =>
      }
    }
  }


  /**
   * HDFS Path and output file name will be in this format:
   * /base/hdfs/path/tablename_snapmode_unixepoch_taskid
   *
   * @param conf the job Configuration
   * @return the HDFS Path of the file where ETL data will be persisted
   */
  private def getOutputPath(conf: Configuration): Path = {
    new Path(
      conf.get(ES_DB_BASE_OUTPUT_PATH, "ERROR_NO_BASE_PATH") + "/" +
      List(
        conf.get(ES_DB_TABLE_NAME, "ERROR_NO_TABLE_NAME_SUPPLIED"),
        conf.get(ES_DB_MODE, "ERROR_NO_MODE_SUPPLIED"),
        conf.get(ES_JOB_TIMESTAMP, "ERROR_NO_JOB_TIMESTAMP_SUPPLIED"), 
        conf.getInt(ES_THIS_TASK_ID, ES_ERROR_CODE).toString
      ).mkString("_")
    )
  }


  private def closeResources(): Unit = {
    if (stream != null) { stream.hflush ; stream.close }
    if (dfs != null) { dfs.close }
    if (rs != null) { rs.close }
    if (db != null) { db.close }
  }
}
