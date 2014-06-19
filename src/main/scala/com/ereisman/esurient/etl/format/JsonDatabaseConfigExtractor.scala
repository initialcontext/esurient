package com.ereisman.esurient.etl.format


import com.ereisman.esurient.EsurientConstants._

import com.codahale.jerkson._

import java.util.Properties

import org.apache.log4j.Logger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DistributedFileSystem

import scala.collection.JavaConversions._
import scala.io.Source


object JsonDatabaseConfigExtractor {
    val Side = """(.*)_[A-Z]""".r
    val LOG = Logger.getLogger(classOf[JsonDatabaseConfigExtractor])
}


/**
 * Utility class for loading and parsing your database config file and
 * injecting the values into a Java properties file in a form the Esurient
 * ETL jobs can process to assign task work. Will be written to HDFS by caller.
 *
 * You will probably need to implement one of these to suit your own needs.
 *
 * This example implementation assumes a JSON-based map of keys and values under parent
 * key "database". Each key represents a DB host, each value the associated metadata.
 */
class JsonDatabaseConfigExtractor extends DatabaseConfigExtractor {
  import com.ereisman.esurient.etl.format.JsonDatabaseConfigExtractor._


  /**
   * Only public method - does the actual parse of HDFS file and injection of
   * DB connection metadata into the Java Properties object supplied by the caller.
   *
   * The term "shard" is tossed around here, but in DB's that only have a single
   * host or database entry, this all still works just fine.
   *
   * @param dfs   the HDFS FileSystem used to load the database properties file.
   * @param conf  the Hadoop Configuration used to configure/parameterize this operation.
   * @param props the Java Properties object we are injecting database metadata into.
   */
  override def extractDatabaseConfigs(dfs: DistributedFileSystem, conf: Configuration, props: Properties): Unit = {
    val db = conf.get(ES_DB_DATABASE, "ERROR_NO_DATABASE_SUPPLIED")
    val shardMap = loadDbConfig(dfs, conf)
    val shardsByBaseName = shardMap.keys
      .groupBy { key => key match { case Side(baseOfKey) => baseOfKey ; case _ => key } }
      .filter { entry => entry._1.startsWith(db) }

    LOG.info("Applying db configs to " + (shardsByBaseName.keys.size) + " task configurations for snapshot of DB: " + db)

    // first shard is chosen to provide values that are the same in all metadata entries
    val globalMap = shardMap(shardsByBaseName(shardsByBaseName.keys.first).first)
    props.setProperty(ES_DB_USERNAME, globalMap("user"))
    props.setProperty(ES_DB_PORT, globalMap("port"))

    // these props will be different for each shard's metadata of the db
    shardsByBaseName.keys.toSeq.sorted[String].zipWithIndex.foreach {
      entry => injectProperties(props, shardMap, shardsByBaseName(entry._1), entry._2)
    }
  }


  private def injectProperties(props: Properties, shardMap: Map[String, Map[String, String]],
                               shardKeys: Iterable[String], taskId: Int): Unit = {
    val suffix = "." + taskId
    val dbMap = shardMap(shardKeys.first)
    Map[String, String](
      ES_DB_HOSTNAME + suffix   -> { shardKeys.map { k => shardMap(k)("host") }.mkString(",") },
      ES_DB_DBNAME + suffix     -> dbMap("dbname")
    ).map { entry => props.setProperty(entry._1, entry._2) }
  }


  private def loadDbConfig(dfs: DistributedFileSystem, conf: Configuration): Map[String, Map[String, String]] = {
    val dbConfPath = new Path(conf.get(ES_DB_CONFIG_FILE_PATH, "ERROR_NO_DATABASE_CONFIG_PATH_FOUND"))
    val javaMap: java.util.Map[String, Any] =
      Json.parse[java.util.LinkedHashMap[String, Any]](
        Source.fromInputStream(dfs.open(dbConfPath), "UTF-8")
      )
    // convert map to Scala before returning
    parseDbConfigFile(javaMap.toMap)
  }


  // Map the text-based database config file and format into more accessible map layout
  private def parseDbConfigFile(dbConf: Map[String, Any]): Map[String, Map[String, String]] = {
    val dbMeta: java.util.Map[String, Any] =
      dbConf("database").asInstanceOf[java.util.LinkedHashMap[String, Any]]

    dbMeta.toMap.keys.map { (key: String) =>
      val raw = dbMeta(key).asInstanceOf[String]
      val sep = raw.contains(";port") match { case true => ";" ; case false => "\\s+" }
      
      // map of (db_prop_key  => db_prop_value)
      val map = raw.split(sep).map { token: String =>
        val entry = token.split("=")
        // "host" key is prefixed as "dbtype:host", strip it
        val key = if (entry(0).endsWith("host")) "host" else entry(0)
        (key, entry(1))
      }.toMap

      // map of database listings (db_name => db_props_map)
      (key, map)
    }.toMap
  }

}
