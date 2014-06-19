package com.ereisman.esurient.etl.format


import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration

import com.ereisman.esurient.EsurientConstants._


/**
 * Utility class to abstract the plugging of DB config extractor modules.
 *
 * @param a Hadoop Configuration that can parameterize the extractor selection.
 */
object DatabaseConfigExtractorFactory {
  val JsonExtractor = "com.ereisman.esurient.etl.format.JsonDatabaseConfigExtractor"
  //val YourExtractor = "your.awesome.class.here.YourDatabaseConfigExtractor"


  def getConfigExtractor(conf: Configuration): DatabaseConfigExtractor = {
    conf.get(ES_DB_DATABASE_CONFIG_EXTRACTOR_TYPE, JsonExtractor) match {
      case JsonExtractor => new JsonDatabaseConfigExtractor
      //case YourExtractor => new YourDatabaseConfigExtractor
      case _ => throw new RuntimeException("DatabaseConfigExtractor implementation selected is not supported yet.")
    }
  }
}
