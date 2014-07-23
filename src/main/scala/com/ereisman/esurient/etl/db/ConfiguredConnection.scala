package com.ereisman.esurient.etl.db


import java.sql.Statement

import java.util.Properties


trait ConfiguredConnection {

  /**
   * MySQL, Postgres, etc. JDBC drivers can accept different custom properties.
   * Subclasses should override this.
   *
   * @param user the user name to submit for the database connection.
   * @param pass the password to submit for the database connection.
   *
   * @returns a Java Properties object the framework will use to configure the connection.
   */
  def getConnectionProperties(user: String, pass: String): Properties = {
    val props = new Properties
    Map(
      "username" -> user,
      "password" -> pass
    ).map { entry => props.setProperty(entry._1, entry._2) }

    props
  }


  /**
   * Perform JDBC vendor-specific functions on a newly-created db Statement object.
   * @param statement the Statement in question.
   */
  def configureStatement(statment: Statement): Unit = { }

}
