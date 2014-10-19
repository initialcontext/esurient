package com.ereisman.esurient.etl


import java.sql.{ResultSet,SQLException}
import java.io.OutputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,Path,PathFilter}
import org.apache.log4j.Logger

import com.ereisman.esurient.etl.db.{Database,DatabaseFactory}
import com.ereisman.esurient.EsurientConstants._
import com.ereisman.esurient.etl.format.{EtlOutputFormatter,OutStreamGenerator}
import com.ereisman.esurient.util.EsurientStats


object EsurientEtlDriver {
  val LOG = Logger.getLogger(classOf[EsurientEtlDriver])
  val RECORDS_PROCESSED_METRICS_MSG_FORMAT = "hadoop.etl.snapshot.records.processed.%s.task%s %s"
  val REPORT_METRIC_PERIOD = 10000L
}


/**
 * Called from the EsurientEtlTask#execute method, this drives
 * the ETL job, manages the lifecycle of the DB connections, etc.
 * Retries are handled by the framework.
 *
 * It is assumed that an earlier run of EsurientEtlSetupScript
 * (perhaps via bin/setup-etl-job) has produced a Java Properties
 * file on HDFS which this driver will use to configure this snapshot.
 */
class EsurientEtlDriver(
    val conf: Configuration,
    val stats: EsurientStats,
    val outputFormatter: EtlOutputFormatter,
    val streamGenerator: OutStreamGenerator
  ) {
  import com.ereisman.esurient.etl.EsurientEtlDriver._

  // The contents of these fields are subject to reinitialization and their
  // lifecycles are managed by this task. Therefore, they are mutable.
  private var db: Database                  = null
  private var rs: ResultSet                 = null
  private var dfs: FileSystem               = null
  private var stream: OutputStream          = null
  private var formatter: EtlOutputFormatter = null
  private var chunkNum: Int                 = 0
  private var chunkSize: Long               = 0L
  private var maxChunkSize: Long            = streamGenerator.getMaxChunkSize

  ///// EXECUTE THE JOB /////
  try {
    performSnapshot
  } catch {
    case ex: Throwable => blowUp(LOG, ex)
  } finally {
    closeResources
  }
  ///// CONSTRUCTOR ENDS HERE /////


  // (re)initialize class state for this snap attempt, execute the snapshot
  private def performSnapshot: Unit = {
    // init the FileSystem and set up the first output file chunk
    initNextFileChunk

    // obtain db connection and execute query
    db = DatabaseFactory.getDatabase(conf)
    rs = submitQuery

    // set up metrics counter for records processed
    var recordCounter: Long = 0L

    // parse & clean each record, then write to HDFS
    do {
      while (rs.next && !rs.isAfterLast) {
        chunkSize += outputFormatter.formatRecord(rs, stream)
        recordCounter += 1L
        checkPingMetricsAndChunkSize(recordCounter)
      }
    } while (thereAreMoreResults)

    // hit metrics one more time with final total
    pingMetrics(recordCounter)
  }


  // check if chunk file is too big or we need to ping metrics every
  // REPORT_METRIC_PERIOD record writes from DB => output stream.
  private def checkPingMetricsAndChunkSize(counter: Long): Unit = {
    if (counter % REPORT_METRIC_PERIOD == 0) {
      pingMetrics(counter)
      checkExceedChunkSize
    }
  }


  private def pingMetrics(counter: Long): Unit = {
    stats.pushMetric(
      Map[String, String](
        "msgFormat"   -> RECORDS_PROCESSED_METRICS_MSG_FORMAT,
        "metricValue" -> counter.toString,
        "timeStamp"   -> (System.currentTimeMillis / 1000L).toString
      )
    )
  }


  // start a new file chunk if the one we're writing has gotten too big
  private def checkExceedChunkSize: Unit = {
    if (maxChunkSize != ES_DB_NO_CHUNK_SIZE_SET && chunkSize > maxChunkSize) initNextFileChunk
  }


  // This can be called multiple times to start each new chunk
  // if the max chunk size is set for this output stream type.
  private def initNextFileChunk: Unit = {
    initHdfs
    if (stream != null) { stream.flush; stream.close ; stream = null }
    chunkSize = 0L
    chunkNum += 1
    stream = streamGenerator.getOutputStream(dfs, getOutputPath)
  }


  private def initHdfs: Unit = {
    if (dfs == null) { dfs = EtlUtils.getDfs(conf) }
  }


  private def thereAreMoreResults: Boolean = {
    LOG.info("Attempting to fetch next ResultSet...")
    rs.getStatement.getMoreResults match {
      case true => rs = rs.getStatement.getResultSet ; true
      case _    => LOG.info("No more ResultSets to process.") ; false
    }
  }


  private def submitQuery: ResultSet = {
    conf.get(ES_DB_MODE, "ERROR_NO_MODE_SUPPLIED") match {
      case ES_DB_BOOTSTRAP_MODE => db.fullTableSnapshot.get
      case ES_DB_UPDATE_MODE    => db.updateTableSnapshot.get
      case _                    =>
        throw new RuntimeException("No job mode (bootstrap or update) supplied in Configuration, aborting.")
    }
  }


  private def blowUp(log: Logger, ex: Throwable): Unit = {
    EtlUtils.logException(LOG, ex)
    cleanupPartialOutput
    throw new RuntimeException(ex)
  }


  private def cleanupPartialOutput: Unit = {
    if (stream != null) { stream.flush ; stream.close }
    initHdfs
    for (val target <- dfs.listStatus(new Path(getBasePathString), allChunksFilter)) {
      dfs.delete(target.getPath, false)
    }
  }


  // get all chunk files from this timestamped run, for this table, by this task ID
  private def allChunksFilter: PathFilter = {
    val p = getOutputPath.toString
    val filterGlob = p.substring(0, p.indexOf("chunk"))

    new PathFilter { override def accept(path: Path): Boolean = { path.toString.contains(filterGlob) } }
  }


  private def getBasePathString: String = {
      conf.get(ES_DB_BASE_OUTPUT_PATH, "ERROR_NO_BASE_PATH") + "/" +
      conf.get(ES_DB_TABLE_NAME, "ERROR_NO_TABLE_NAME_SUPPLIED") +
      "/data"
  }


  /**
   * Generates a full path/filename on HDFS for the current chunk we will write.
   * HDFS Path and output file names will be in this format:
   * /base/hdfs/path/to/tablename/data/tablename_snapmode_unixepoch_task_N_chunk_N.suffix
   *
   * Where "data" dir is hardcoded to separate job Properties file from data files.
   * This is because std Hadoop post-processing jobs will want to read whole dir of
   * gzip files at once, having props/schema files in there will confuse them.
   *
   * @return the HDFS Path of the file where ETL data will be persisted
   */
  private def getOutputPath: Path = {
    new Path(
      getBasePathString + "/" +
      List(
        conf.get(ES_DB_TABLE_NAME, "ERROR_NO_TABLE_NAME_SUPPLIED"),
        conf.get(ES_DB_MODE, "ERROR_NO_MODE_SUPPLIED"),
        conf.get(ES_JOB_TIMESTAMP, "ERROR_NO_JOB_TIMESTAMP_SUPPLIED"),
        "task",
        conf.getInt(ES_THIS_TASK_ID, ES_ERROR_CODE).toString,
        "chunk",
        chunkNum
      ).mkString("_") +
      streamGenerator.getFileTypeSuffix
    )
  }


  private def closeResources: Unit = {
    if (stream != null) { stream.flush ; stream.close }
    if (dfs != null) { dfs.close }
    if (rs != null) { rs.close }
    if (db != null) { db.close }
  }
}
