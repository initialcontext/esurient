package com.ereisman.esurient.db


import org.apache.hadoop.conf.Configuration


object DatabaseFactory {

  /**
   * Construct a database manager object from some input configurations.
   *
   * @param conf a Hadoop Configuration object that has been prepopulated
   *             with DB metadata sufficient to build Database objects.
   * @return a Database object to manage the ETL task.
   */
  def getDatabase(taskId: Int, conf: Configuration): Database = {
    conf.get("esurient.db.type", "ERROR_NO_DB_TYPE_SET") match {
      case "mysql"      => conf.getBoolean("esurient.sharded.db", false) match {
        case true          => new ShardedMySqlDatabase(taskId, conf)
        case _             => new MySqlDatabase(taskId, conf)
      }
      case "pgsql"      => new PostgresDatabase(taskId, conf)
      case str: String  => throw new RuntimeException("Esurient does not implement db connections of type: " + str)
    }
  }
}
