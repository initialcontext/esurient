package com.ereisman.esurient.etl.db


import com.ereisman.esurient.EsurientConstants._
import org.apache.hadoop.conf.Configuration


object DatabaseFactory {

  /**
   * Factory for Database objects.
   *
   * @param taskId the unique ID for this Esurient task, used by the Database
   *               object to query the Configuration for its query metadata.
   *
   * @param conf a Hadoop Configuration object that has been prepopulated
   *             with DB metadata sufficient to build Database objects.
   *
   * @return a Database object to manage the ETL task.
   */
  def getDatabase(conf: Configuration): Database = {
    conf.get(ES_DB_TYPE, "ERROR_NO_DB_TYPE_SET") match {
      case "mysql"      => new MySqlDatabase(conf)
      
      case "pgsql"      => new PostgresDatabase(conf)
      
      case wtf: Any     =>
        throw new RuntimeException("Esurient does not implement db connections of type: " + wtf.toString)
    }
  }
}
