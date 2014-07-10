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
import com.ereisman.esurient.etl.Utils


object JdbcDatabase {
  val LOG = Logger.getLogger(classOf[JdbcDatabase])
}


/**
 * Abstracts awway DB-agnostic JDBC driver boilerplate from the DB-specific subclasses.
 */
class JdbcDatabase(conf: Configuration, driver: String, val jdbcScheme: String) extends Database with ConfiguredConnection {
  import com.ereisman.esurient.etl.db.JdbcDatabase.LOG
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
      conf.getInt(ES_JOB_TIMESTAMP, Now) - conf.getInt(ES_DB_UPDATE_WINDOW_SECS, ES_DB_WINDOW_SECS_DEFAULT)
    
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

    queryWithRetries(produceResultSet, "SELECT * FROM " + tableName + " LIMIT 1", retries) match {
      case Some(resultSet) => try {
          resultSet.next
          generateSchemaJson(resultSet.getMetaData, tableName)
        } finally {
          if (resultSet != null) { resultSet.close }
        }
      case _               => "[ ]"
    }
  }

  /**
   * Close the Connection cleanly. Caller/Owner of Database objects must manage
   * the underlying connection's lifecycle themselves - remember to call this!
   */
  override def close: Unit = {
    connection match {
      case c: Connection => c.close
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

  private def produceResultSet(query: String): Option[ResultSet] = {
    val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    statement.setFetchSize(Integer.MIN_VALUE) // this will stream records rather than host in-mem
    Some(statement.executeQuery(query))
  }

  private def generateSchemaJson(metaData: ResultSetMetaData, tableName: String): String = {
    // get a Set of primary key names for this table to include in the returned JSON
    val primaryKeys = queryWithRetries(getPrimaryKeysFromConnection, tableName, retries) match {
      case Some(resultSet) => {
        try {
          val names = scala.collection.mutable.Set[String]()
          while (resultSet.next) {
            names += resultSet.getString("COLUMN_NAME")
          }
          names.toSet // make immutable
        } finally {
          if (resultSet != null) { resultSet.close }
        }
      }
      case None            => scala.collection.Set[String]()
    }
    // Generate Schema JSON, transform to String
    Json.generate(
      (1 to metaData.getColumnCount).map { index: Int =>
        Map[String, Any](
          "column" -> index,  
          "name" -> metaData.getColumnName(index),
          "type" -> metaData.getColumnTypeName(index),
          "class" -> metaData.getColumnClassName(index),
          "primary" -> primaryKeys.contains(metaData.getColumnName(index))
        )
      }.toList ++ appendShardId(metaData.getColumnCount + 1)
    ).toString
  }

  // if this is a sharded table, append the shard id to the schema
  private def appendShardId(columnIndex: Int): List[Map[String, Any]] = {
      conf.getBoolean(ES_DB_SHARDED_TABLE, false) match {
        case true   => {
          List(
            Map[String, Any](
              "column"  -> columnIndex,
              "name"    -> "shard_id",
              "type"    -> "INT UNSIGNED",
              "class"   -> "java.lang.Long",
              "primary" -> "false"
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
    connection.setReadOnly(true)
    // return the connection
    connection
  }

  private def handleFatal(log: Logger, ex: Exception): Option[ResultSet] = {
    close
    Utils.logFatal(LOG, ex)
    None
  }

  private def openConnection: Unit = {
    connection = connection match {
      case c: Connection if (c.isValid(0)) => c
      case _ => close ; getConnection
    }
  }

  private def handleRetryException(log: Logger, sqlEx: SQLException) {
    LOG.warn("Query Attempt FAILED with error trace:")
    Utils.logException(log, sqlEx)
    LOG.warn("Retrying Query...")
  }
}
