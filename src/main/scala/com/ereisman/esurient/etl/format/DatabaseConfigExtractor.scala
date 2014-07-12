package com.ereisman.esurient.etl.format

import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
 * The basis for pluggable modules that handle the specifics when loading and parsing
 * your database config file. Subclasses should inject the values into a Java Properties object.
 * The calling code will write this Properties object out to HDFS for Esurient ETL jobs to reference.
 *
 * Assumptions:
 *  1) the Configuration contains all the metadata we need to know where the DB config file is
 *
 *  2) the DistributedFileSystem reference is to HDFS where this Db config file is stored
 *
 *  3) the Properties object passed in should be injected full of formatted values from the
 *     DB config file - in a format that Esurient ETL jobs will understand when these
 *     properties read from HDFS during a snapshot job later on.
 */
trait DatabaseConfigExtractor {
 
  /**
   * Utility method to load the DB config file from HDFS, parse it into data structures, and
   * use it to assign keys and values into the Java Properties object provided by the caller.
   *
   * @param dfs   the HDFS FileSystem used to load the database properties file.
   * @param conf  the Hadoop Configuration used to configure/parameterize this operation.
   * @param props the Java Properties object we are injecting database metadata into.
   */
  def extractDatabaseConfigs(dfs: FileSystem, conf: Configuration, props: Properties): Unit = { }

}
