package com.ereisman.esurient.etl


import java.sql.{ResultSet,SQLException}
import java.io.{OutputStream,BufferedOutputStream,IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.compress.{GzipCodec,CompressionCodecFactory}
import org.apache.log4j.Logger

import com.ereisman.esurient.etl.db.{Database,DatabaseFactory}
import com.ereisman.esurient.EsurientConstants._
import com.ereisman.esurient.etl.format.EtlOutputFormatter


object EsurientEtlDriver {
  val LOG = Logger.getLogger(classOf[EsurientEtlDriver])
}


/**
 * Called from the EsurientEtlTask#execute method, this drives
 * the ETL job, manages the lifecycle of the DB connections, etc.
 * Retries are handled by the framework (i.e. new mapper is spawned
 * to handle this portion of the ETL task if this one dies.)
 *
 * It is assumed that an earlier run of EsurientEtlSetupScript
 * (perhaps via bin/setup-etl-job) has produced a Java Properties
 * file on HDFS which this driver will use to configure this snapshot.
 */
class EsurientEtlDriver(val conf: Configuration, val outputFormatter: EtlOutputFormatter) {
  import com.ereisman.esurient.etl.EsurientEtlDriver._

  // The contents of these fields are subject to reinitialization during retries,
  // and their lifecycles are managed by this task. Therefore, they are mutable.
  var db: Database = null
  var rs: ResultSet = null
  var dfs: FileSystem = null
  var stream: OutputStream = null
  var formatter: EtlOutputFormatter = null
  var outPath: Path = null

  ///// EXECUTE THE JOB /////
  try {
    performSnapshot(conf)
  } catch {
    case ex: Throwable => blowUp(LOG, ex)
  } finally {
    closeResources
  }
  ///// CONSTRUCTOR ENDS HERE /////


  // (re)initialize class state for this snap attempt, execute the snapshot
  private def performSnapshot(conf: Configuration): Unit = {
    // initialize filesystem resources
    initializeFsResources

    // obtain db connection and execute query
    db = DatabaseFactory.getDatabase(conf)
    rs = submitQuery(conf)

    // parse & clean each record, then write to HDFS
    do {
      while (rs.next && !rs.isAfterLast) {
        outputFormatter.formatRecord(rs, stream)
      }
    } while (thereAreMoreResults)
  }


  private def initializeFsResources: Unit = {
    dfs = Utils.getDfs(conf)
    outPath = getOutputPath(conf)
    
    val bos = new BufferedOutputStream(dfs.create(outPath, true), BUFFER_SIZE)
    val codecClass = conf.get(ES_DB_OUTPUT_COMP_TYPE, ES_DB_OUTPUT_COMP_TYPE_DEFAULT)
    
    stream = (new CompressionCodecFactory(conf)).getCodecByClassName(codecClass) match {
      case gzc: GzipCodec => gzc.createOutputStream(bos) // TODO: break this out into pluggable comp manager class
      case _                   => bos
    }
  }


  private def thereAreMoreResults: Boolean = {
    LOG.info("Attempting to fetch next ResultSet...")
    rs.getStatement.getMoreResults match {
      case true => rs = rs.getStatement.getResultSet ; true
      case _    => LOG.info("No more ResultSets to process.") ; false
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


  private def blowUp(log: Logger, ex: Throwable): Unit = {
    Utils.logException(LOG, ex)
    cleanupPartialOutput
    throw new RuntimeException(ex)
  }


  private def cleanupPartialOutput: Unit = {
    if (stream != null) { stream.flush ; stream.close }
    if (dfs != null) {
      dfs.exists(outPath) match {
        case true => dfs.delete(outPath, false) ; Thread.sleep(ES_DB_QUICK_PAUSE_MILLIS)
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
      conf.get(ES_DB_TABLE_NAME, "ERROR_NO_TABLE_NAME_SUPPLIED") + "/" +
      List(
        conf.get(ES_DB_TABLE_NAME, "ERROR_NO_TABLE_NAME_SUPPLIED"),
        conf.get(ES_DB_MODE, "ERROR_NO_MODE_SUPPLIED"),
        conf.get(ES_JOB_TIMESTAMP, "ERROR_NO_JOB_TIMESTAMP_SUPPLIED"),
        conf.getInt(ES_THIS_TASK_ID, ES_ERROR_CODE).toString
      ).mkString("_") +
      ".gz" // TODO: make GZIP pluggable, not default. add this method to a "compression manager" class ASAP
    )
  }


  private def closeResources(): Unit = {
    if (stream != null) { stream.flush ; stream.close }
    if (dfs != null) { dfs.close }
    if (rs != null) { rs.close }
    if (db != null) { db.close }
  }
}
