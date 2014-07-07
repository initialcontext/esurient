package com.ereisman.esurient.etl


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.log4j.Logger

import java.sql.ResultSetMetaData
import java.util.Properties

import scala.io.Source
import scala.annotation.tailrec

import com.ereisman.esurient.EsurientConstants._
import com.ereisman.esurient.etl.db.{Database,DatabaseFactory}
import com.ereisman.esurient.etl.format.DatabaseConfigExtractor


object EsurientEtlMetadataManager {
  val LOG = Logger.getLogger(classOf[EsurientEtlMetadataManager])
}

/**
 * Scala utility that generates JSON-based table schema file and Esurient
 * job.properties on HDFS in preparation for an EsurientEtlTask snapshot
 * of the table named in the command-line args.
 *
 * Note: This assumes the use of <code>--dbConfig</code> which specifies the
 * location of a file on HDFS that contains all database connection metadata
 * that must be integrated into the generated job properties file. That file
 * will be used to configure table snapshot jobs run using EsurientEtlTask.
 *
 * USAGE:
 * <code>
 * java -cp /path/to/esurient-VERSION-jar-with-dependencies.jar \
 *   com.ereisman.ersurient.examples.EsurientEtlSetupScript \
 *   --dbConfig /hdfs/path/to/db/config/file.json \
 *   --outputPath /hdfs/path/to/table/dir \
 *   --database db_name \
 *   --dbType mysql|postgres \
 *   --table table_name \
 *   --mode bootstrap|update \
 *   --dbPass password \
 *   --updateCol col_name \
 *   --window updateWindowSecs
 * </code>
 *
 * Where 'VERSION' is whatever your current Esurient build produces.
 *
 * Note: the <code>--window</code> and <code>--updateCol</code>
 *       args are optional (used only in 'update' mode)
 */
class EsurientEtlMetadataManager(val args: Array[String], val conf: Configuration, val extractor: DatabaseConfigExtractor) {
  import com.ereisman.esurient.etl.EsurientEtlMetadataManager.LOG

  parseArgsIntoConf(args.toList)
  val dfs = Utils.getDfs(conf)

  try {
    checkCreateOutputPath
    updateJobPropertiesFile
    updateTableSchemaFile

    LOG.info("Successfully updated metadata for table: " + conf.get(ES_DB_TABLE_NAME, "UNKNOWN"))
  } finally {
    dfs.close
  }
  ///// END CONSTRUCTOR /////


  // publish a JSON-based table schema to HDFS in the same dir as the snapshot job properties
  private def updateTableSchemaFile: Unit = {
    val schemaPath = getSchemaFile
    val stream = dfs.create(schemaPath, true)

    LOG.info("Updating table schema file: " + schemaPath)
    try {
      // CLI args are populated into Configuration so Database can connect & get JSON schema
      val str = DatabaseFactory.getDatabase(conf).getTableSchema.getBytes("UTF-8")
      val len = str.length
      stream.write(str, 0, len)
    } finally {
      stream.hsync
      stream.close
    }
  }


  // read latest database config JSON and rewrite the job properties for the next snap run
  private def updateJobPropertiesFile: Unit = {

    // inject values into a Properties object that will become the ETL job's config file
    val props: Properties = getEtlBaseProperties
    extractor.extractDatabaseConfigs(dfs, conf, props)

    // write the new ETL job properties file out, configuring a table snapshot
    val propsPath = getJobPropertiesFile
    val stream = dfs.create(propsPath, true)
    try {
      LOG.info("Updating job properties file for table snapshot: " + propsPath)
      val str = writeEtlJobProperties(props).getBytes("UTF-8")
      val len = str.length
      stream.write(str, 0, len)
      prepConfigurationForDbConnect(props)
    } finally {
      stream.hsync
      stream.close
    }
  }


  private def writeEtlJobProperties(props: Properties): String = {
    import scala.collection.JavaConversions._
    props.iterator.toMap.map { entry => entry._1.toString + "=" + entry._2.toString }.mkString("\n")
  }


  // Put a few of the db config values into conf for use later in schema file generation.
  // These tweaks to the Configuration are for this script only, they never propagate into
  // the generated job properties file. "task 1" is a dummy task for Database object to read.
  private def prepConfigurationForDbConnect(props: Properties): Unit = {
    conf.set(ES_THIS_TASK_ID, "1") // fake this out for Database connection later
    conf.set(ES_DB_HOSTNAME + ".1", props.getProperty(ES_DB_HOSTNAME + ".1", "ERROR_NO_HOSTNAME"))
    conf.set(ES_DB_DBNAME + ".1", props.getProperty(ES_DB_DBNAME + ".1", "ERROR_NO_DBNAME"))
    conf.set(ES_DB_PORT, props.getProperty(ES_DB_PORT, "ERROR_NO_PORT"))
    conf.set(ES_DB_USERNAME, props.getProperty(ES_DB_USERNAME, "ERROR_NO_USERNAME"))
    conf.set(ES_DB_SHARDED_TABLE, props.getProperty(ES_DB_SHARDED_TABLE, "false"))
    // ES_DB_PASSWORD came in as a cmd-line argument
  }


  private def getEtlBaseProperties: Properties = {
    val props = new Properties
    val error = "COMMAND_LINE_ARG_MISSING"

    Map[String, String](
      // System configs
      "mapred.child.java.opts"  -> "-Xmx2G -Xms2G",
      // ETL-specific configs
      ES_DB_PASSWORD            -> conf.get(ES_DB_PASSWORD, error),
      ES_DB_MODE                -> conf.get(ES_DB_MODE, error),
      ES_DB_TABLE_NAME          -> conf.get(ES_DB_TABLE_NAME, error),
      ES_DB_BASE_OUTPUT_PATH    -> conf.get(ES_DB_BASE_OUTPUT_PATH, error),
      ES_DB_DATABASE            -> conf.get(ES_DB_DATABASE, error),
      ES_DB_TYPE                -> conf.get(ES_DB_TYPE, error),
      // TODO: make this test pluggable - there are many better ways to determine if table is sharded or not
      ES_DB_SHARDED_TABLE       -> { if (conf.get(ES_DB_DATABASE, error).contains("shard")) "true" else "false" },
      ES_DB_UPDATE_WINDOW_SECS  -> conf.get(ES_DB_UPDATE_WINDOW_SECS, error),
      ES_DB_UPDATE_COLUMN       -> conf.get(ES_DB_UPDATE_COLUMN, ES_DB_UPDATE_COLUMN_DEFAULT),
      // General EsurientTask boilerplate
      ES_JOB_NAME               -> composeJobName,
      ES_TASK_CLASS_NAME        -> "com.ereisman.esurient.examples.EsurientEtlTask",
      ES_TASK_AUTO_HEARTBEAT    -> "true",
      ES_LOG_HEARTBEATS         -> "true"
    ).map { entry => props.setProperty(entry._1, entry._2) }
    
    props
  }


  // make a job name for the JT to display that tells us something useful about the job run.
  def composeJobName: String = {
    List(
      conf.get(ES_DB_MODE, "MODE_UNKNOWN"),
      "table snapshot:",
      conf.get(ES_DB_TABLE_NAME, "TABLE_UNKNOWN")
    ).mkString(" ")
  }


  // Make sure the base directories exist for this table's
  // snapshot ETL output, schema file, and job properties file.
  private def checkCreateOutputPath: Boolean = {
    val path = {
      List(
        conf.get(ES_DB_BASE_OUTPUT_PATH, "ERROR_NO_BASE_OUTPUT_PATH_SUPPLIED"),
        conf.get(ES_DB_TABLE_NAME, "ERROR_NO_TABLE_NAME_SUPPLIED")
      ).mkString("/")
    }
    dfs.mkdirs(new Path(path), new FsPermission("777"))
  }


  private def getOutputFile(suffix: String): Path = {
    new Path(
      List(
        conf.get(ES_DB_BASE_OUTPUT_PATH, "ERROR_NO_BASE_OUTPUT_PATH_SUPPLIED"),
        conf.get(ES_DB_TABLE_NAME, "ERROR_NO_TABLE_NAME_SUPPLIED"),
        conf.get(ES_DB_TABLE_NAME, "ERROR_NO_TABLE_NAME_SUPPLIED")
      ).mkString("/") + suffix
    )
  }


  private def getSchemaFile: Path = getOutputFile(ES_DB_SCHEMA_FILE_SUFFIX)


  private def getJobPropertiesFile: Path = getOutputFile(ES_DB_JOB_PROPS_FILE_SUFFIX)


  @tailrec private def parseArgsIntoConf(args: List[String]): Unit = {
    args match {
      case Nil                                => return
      case "--dbConfig" :: dbConf :: tail     => conf.set(ES_DB_CONFIG_FILE_PATH, dbConf)
      case "--table" :: tableName :: tail     => conf.set(ES_DB_TABLE_NAME, tableName)
      case "--outputPath" :: outPath :: tail  => conf.set(ES_DB_BASE_OUTPUT_PATH, outPath)
      case "--database" :: db :: tail         => conf.set(ES_DB_DATABASE, db)
      case "--dbType" :: dbtype :: tail       => conf.set(ES_DB_TYPE, dbtype)
      case "--dbPass" :: pass :: tail         => conf.set(ES_DB_PASSWORD, pass)
      case "--mode" :: mode :: tail           => conf.set(ES_DB_MODE, mode)
      case "--window" :: secs :: tail         => conf.setInt(ES_DB_UPDATE_WINDOW_SECS, secs.toInt)
      case "--updateCol" :: col :: tail       => conf.set(ES_DB_UPDATE_COLUMN, col)
      case _                                  => // keep going
    }
    parseArgsIntoConf(args.tail)
  }
}
