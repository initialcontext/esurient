package com.ereisman.esurient.etl.db


import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration

import java.sql.Statement
import java.util.Properties



object PostgresDatabase {
  val LOG = Logger.getLogger(classOf[PostgresDatabase])
}


class PostgresDatabase(val conf: Configuration) extends JdbcDatabase(conf, "org.postgresql.Driver", "jdbc:postgresql://") {
  import com.ereisman.esurient.etl.db.PostgresDatabase.LOG


  override def getConnectionProperties(user: String, pass: String): Properties = {
    val props = new Properties
    Map(
      "user" -> user,
      "password" -> pass
    ).map { entry => props.setProperty(entry._1, entry._2) }

    props
  }


  override def configureStatement(stmt: Statement): Unit = {
    stmt.setFetchSize(100) // total guess, but 0 (in-mem hosted data) is not scalable. No Streaming mode like MySQL driver?!?
  }
}
