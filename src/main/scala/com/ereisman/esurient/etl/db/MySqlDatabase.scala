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

import com.ereisman.esurient.EsurientConstants._


object MySqlDatabase {
  val LOG = Logger.getLogger(classOf[MySqlDatabase]) 
}


class MySqlDatabase(conf: Configuration) extends JdbcDatabase(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://") {
  import com.ereisman.esurient.etl.db.MySqlDatabase.LOG


  // Custom connection config settings for MySQL type JDBC connection.
  override def getConnectionProperties(userName: String, passWord: String): Properties = {
    val props = new Properties
    Map[String, String](
      "user" -> userName,
      "password" -> passWord,
      "useUnbufferedInput" -> "false",
      "connectTimeout" -> ES_DB_CONNECTION_TIMEOUT_DEFAULT.toString,
      "cachePrepStmts" -> "true",
      "cacheCallableStmts" -> "true",
      "cacheServerConfiguration" -> "true",
      "useLocalSessionState" -> "true",
      "alwaysSendSetIsolation" -> "false",
      "enableQueryTimeouts" -> "false"
    ).foreach { entry => props.setProperty(entry._1, entry._2) } 

    props
  }
}
