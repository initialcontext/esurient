package com.ereisman.esurient.etl.db


import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration


object PostgresDatabase {
  val LOG = Logger.getLogger(classOf[PostgresDatabase])
}


class PostgresDatabase(val conf: Configuration) extends JdbcDatabase(conf, "org.postgresql.Driver", "jdbc:postgres://") {
  import com.ereisman.esurient.etl.db.PostgresDatabase.LOG


  // TODO: set up custom Postgres props for JDBC connection config.
  //override def getConnectionProperties(user: String, pass: String): Properties
}
