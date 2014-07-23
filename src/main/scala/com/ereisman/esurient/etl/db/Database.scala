package com.ereisman.esurient.etl.db


import java.sql.{ResultSet,ResultSetMetaData,Statement}

/**
 * Base for all Database classes. Simple interface to retrieve a RecordSet
 * which can be iterated on to format and push DB record data to HDFS.
 * RecordSetMetaData will allow us to update a JSON-based table schema
* file on HDFS.
 *
 * Note: The caller is responsible for closing the Database object to avoid mem leaks.
 */
trait Database {

  /**
   * The UNIX epoch stamp forming the "end" of the database snapshot window
   * (used in calls to Database#updateTableSnapshot only)
   */
  val Now = ((new java.util.Date).getTime / 1000).asInstanceOf[Int]


  /**
   * Perform a full table bootstrap, pulling all records to HDFS.
   * You many call this again in the event of a fail, but remember
   * to clean up any record written to HDFS first!
   *
   * @returns a RecordSet of the records to write to HDFS. The caller is
   *          reponsible for closing the ResultSet to avoid mem leaks.
   */
  def fullTableSnapshot: Option[ResultSet] = { None }


  /**
   * Perform an update query, pulling in only records updated between Now and
   * a time window specified in the Configuration to incrementally update data on HDFS.
   * Analysts (or a post-processing job) will need to filter the aggregate data to
   * remove duplicate records, keeping only the most recent of each, composing a
   * view of the table being replicated that is accurate up to the latest Esurient update run.
   *
   * @returns a RecordSet of the records to write to HDFS. The caller is
   *          reponsible for closing the ResultSet to avoid mem leaks.
   */
  def updateTableSnapshot: Option[ResultSet] = { None }


  /**
   * Obtain table schema in JSON format, as a String. The caller will presumably write this
   * data to HDFS for downstream post-processing jobs to utilize.
   *
   * @param a String representing a JSONArray of table columns and their metadata.
   */
  def getTableSchema: String = { "[ ]" }


  /**
   * Close the Database connnection. Owner of the Database object must call this to avoid JDBC mem leaks.
   */
  def close: Unit = { }
}
