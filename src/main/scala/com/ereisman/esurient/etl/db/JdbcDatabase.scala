package com.ereisman.esurient.etl.db


import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Statement
import java.sql.SQLException
import java.util.Properties

import com.codahale.jerkson._

import com.ereisman.esurient.EsurientConstants._
import com.ereisman.esurient.etl.EtlUtils


object JdbcDatabase {
  val LOG           = Logger.getLogger(classOf[JdbcDatabase])
  val UPDATE_COLUMN = "UPDATE_COLUMN"
  val DEDUP_COLUMN  = "DEDUP_COLUMN"
}


/**
 * Abstracts awway DB-agnostic JDBC driver boilerplate from the DB-specific subclasses.
 */
class JdbcDatabase(conf: Configuration, driver: String, val jdbcScheme: String) extends Database with ConfiguredConnection {
  import com.ereisman.esurient.etl.db.JdbcDatabase._
  // Initialize the specified JDBC Driver.
  Class.forName(driver).newInstance
  // Dastabase state, some of which might be reset during this object's lifetime.
  protected var connection: Connection = null
  protected val taskId = conf.getInt(ES_THIS_TASK_ID, ES_ERROR_CODE)
  protected val tableName = conf.get(ES_DB_TABLE_NAME, "ERROR_NO_DATABASE_TABLE_NAME_SUPPLIED")
  protected val retries = conf.getInt(ES_DB_RETRIES, ES_DB_RETRIES_DEFAULT)


  /**
   * Obtain all records for the table and database specified in the job properties.
   */
  override def fullTableSnapshot: Option[ResultSet] = {
    LOG.info("Attempting full table bootstrap snapshot on: " + tableName)

    queryWithRetries(produceResultSet, "SELECT * FROM " + tableName, retries)
  }


  /**
   * Obtain all DB records produced within the update window specified in job properties.
   */
  override def updateTableSnapshot: Option[ResultSet] = {
    LOG.info("Attempting update snapshot on table: " + tableName)
    val updateColumn = conf.get(ES_DB_UPDATE_COLUMN, ES_DB_UPDATE_COLUMN_DEFAULT)
    val timeStamp =
      conf.getInt(ES_JOB_TIMESTAMP, Now) - conf.getInt(ES_DB_UPDATE_WINDOW_SECS, ES_DB_UPDATE_WINDOW_SECS_DEFAULT)

    val query = "SELECT * FROM " + tableName + " WHERE " + updateColumn + " >= " + timeStamp
    queryWithRetries(produceResultSet, query, retries)
  }


  /**
   * Generate the JSON-based table schema for use by downstream post-processing jobs to
   * map table metadata to the formatted row data the ETL job will produce.
   *
   * @returns JSONArray as a String containing the table schema information.
   */
  override def getTableSchema: String = {
    LOG.info("Attempting to obtain schema info for table: " + tableName)
    val primarySet = getPrimaryKeySet(tableName)
    generateSchemaJson(primarySet, tableName)
  }


  /**
   * Close the Connection cleanly. Caller/Owner of Database objects must manage
   * the underlying connection's lifecycle themselves - remember to call this!
   *
   * NOTE: The match block is an ugly workaround due to MySQL JDBC Driver 5.0.x
   *       containing a bug in the Connection#isValid method. We don't want to
   *       upgrade to 5.1.x series as it has bigger bug in streaming read mode.
   */
  override def close: Unit = {
    connection match {
      case c: Connection => c.close ; connection = null
      case _             =>
    }
  }


  /**
   * The driver method subclasses will call to do the work.
   *
   * @param func any function accepting a String and optionally returning a JDBC ResultSet.
   * @param query the String to be passed to <code>func</code>, often an SQL query but could be param.
   * @param retries the number of retries to attempt upon connection failures. Defaults to 3.
   *
   * @returns an Option[ResultSet] that should provide each record (row) returned by the query.
   */
  protected def queryWithRetries(func: (String) => Option[ResultSet], query: String, retries: Int): Option[ResultSet] = {
    LOG.info("Query attempt (" + retries + "): " + query)
    try {
      openConnection
      func(query)
    } catch {
      case sqlEx: SQLException => retries match {
          case more: Int if (more > 0) => {
            handleRetryException(LOG, sqlEx)
            queryWithRetries(func, query, more - 1)
          }
          // if out of retries, fail task
          case _ => handleFatal(LOG, sqlEx)
        }
      // if non-JDBC based exception, better fail task
      case ex: Exception => handleFatal(LOG, ex)
    }
  }


  private def getPrimaryKeysFromConnection(tableName: String): Option[ResultSet] = {
    Some(
      connection
      .getMetaData
      .getPrimaryKeys(null, null, tableName)
    )
  }


  private def getPrimaryKeySet(tableName: String): Set[String] = {
    // get a Set of primary key names for this table to include in the returned JSON
    queryWithRetries(getPrimaryKeysFromConnection, tableName, retries) match {
      case Some(rSet)      => {
        try {
          val names = scala.collection.mutable.Set[String]()
          while (rSet.next) { names += rSet.getString("COLUMN_NAME") }
          names.toSet // make immutable
        } finally {
          if (rSet != null) { rSet.close }
        }
      }
      case None            => Set[String]()
    }
  }


  private def produceResultSet(query: String): Option[ResultSet] = {
    val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    // no-op in base class, hopefully overridden in vendor-specific way in subclasses
    configureStatement(statement)
    Some(statement.executeQuery(query))
  }


  private def generateSchemaJson(primaryKeys: Set[String], tableName: String): String = {
    queryWithRetries(produceResultSet, "SELECT * FROM " + tableName + " LIMIT 1", retries) match {
      case Some(rSet) => {
        val metaData = rSet.getMetaData
        try {
          // Generate Schema JSON, transform to String
          Json.generate(
            (1 to metaData.getColumnCount).map { index: Int =>
              val colName = metaData.getColumnName(index)
              Map[String, Any](
                "column"   -> index,  
                "name"     -> colName,
                "type"     -> metaData.getColumnTypeName(index),
                "class"    -> metaData.getColumnClassName(index),
                "primary"  -> primaryKeys.contains(colName),
                "nullable" -> metaData.isNullable(index),
                "other"    -> getOtherMetaData(colName) 
              )
            }.toList ++ appendShardId(metaData.getColumnCount + 1)
          ).toString
        } finally {
          if (rSet != null) { rSet.close }
        }
      }
      case _          => "[ ]"
    }
  }


  private def getOtherMetaData(col: String): String = {
    val updateCol = conf.get(ES_DB_UPDATE_COLUMN, "")
    val dedupCol  = conf.get(ES_DB_DEDUP_COLUMN, "")
    col match {
      case `updateCol` => JdbcDatabase.UPDATE_COLUMN
      case `dedupCol`  => JdbcDatabase.DEDUP_COLUMN
      case _         => ""
    }
  }


  // if this is a sharded table, append the shard id to the schema
  private def appendShardId(columnIndex: Int): List[Map[String, Any]] = {
      conf.getBoolean(ES_DB_SHARDED_TABLE, false) match {
        case true   => {
          List(
            Map[String, Any](
              "column"   -> columnIndex,
              "name"     -> "shard_id",
              "type"     -> "INT",
              "class"    -> "java.lang.Integer",
              "primary"  -> "false",
              "nullable" -> "0",
              "other"    -> ""
            )
          )
        }
        case _      => Nil
      }
  }

  private def getConnection: Connection = {
    val taskId = conf.getInt(ES_THIS_TASK_ID, ES_ERROR_CODE)
    val (hostCsvList, dbName, port, userName, passWord) = {
      val suffix = "." + taskId
      (
        conf.get(ES_DB_HOSTNAME + suffix, "ERROR_NO_HOSTNAME_FOR_TASKID_" + taskId),
        conf.get(ES_DB_DBNAME + suffix, "ERROR_NO_DBNAME_FOR_TASKID_" + taskId),
        conf.get(ES_DB_PORT, "ERROR_NO_PORT"),
        conf.get(ES_DB_USERNAME, "ERROR_NO_USERNAME"),
        conf.get(ES_DB_PASSWORD, "ERROR_NO_PASSWORD")
      )
    }

    // hostnames are CSV list so JDBC will round robin connections
    val url = jdbcScheme + hostCsvList + ":" + port + "/" + dbName
    LOG.info("JDBC Connection URL: " + url + " as db user: " + userName)
    val props = getConnectionProperties(userName, passWord)
    val connection = DriverManager.getConnection(url, props)

    // configure connection for optimized read-only that all JDBC drivers will understand
    connection.setAutoCommit(false)
    connection.setReadOnly(true)

    // return the connection
    connection
  }

  private def handleFatal(log: Logger, ex: Exception): Option[ResultSet] = {
    close
    EtlUtils.logFatal(LOG, ex)
    None
  }

  /*
   * Ugly hack here - we use 'null' as flag that Connection was
   * reset or otherwise invalidated because we can't use Connection#isValid
   * on JDBC MySQL driver 5.0.x series, but we need its streaming features.
   */
  private def openConnection: Unit = {
    connection match {
      case c: Connection => // do nothing, the connection is still instantiated
      case _             => connection = getConnection
    }
    Unit
  }

  private def handleRetryException(log: Logger, sqlEx: SQLException): Unit = {
    LOG.warn("Query Attempt FAILED with error trace:")
    EtlUtils.logException(log, sqlEx)
  }
}
