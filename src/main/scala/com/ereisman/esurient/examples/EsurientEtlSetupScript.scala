package com.ereisman.esurient.examples


import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger

import com.ereisman.esurient.etl.EsurientEtlMetadataManager
import com.ereisman.esurient.etl.format.DatabaseConfigExtractorFactory


/**
 * Executable host container for ETL setup task, passes CLI args and
 * a Hadoop Configuration (which you can customize) and your choice of
 * DatabaseConfigExtractor required to produce a Java Properties object
 * in a form EsurientEtlTask jobs can process, and a JSON-based table
 * schema file that post-processing jobs can use.
 *
 * You will likely want to write your own script driver class like this
 * and plug your own DatabaseConfigExtractor class and Configuration
 * into the EsurientEtlMetadataManager to customize this process.
 *
 * Once the job.properties and table schema JSON file has been generated,
 * it can be used as input to EsurientEtlTask or post-processing jobs
 * indefinitely until some schema or other job change occurs for that table.
 */
object EsurientEtlSetupScript {

  def main(args: Array[String]): Unit = {    
    // pass the Hadoop Configuration in from here to allow for pre-run
    // customization in future implementations of this script before the
    // EsurientEtlMetadataManager takes over. Done here for demo only.
    val conf = new Configuration(true)

    // configure this setup job with DB config extactor - default is JSON example
    val extractor = DatabaseConfigExtractorFactory.getConfigExtractor(conf)

    // execute the ETL job, passing the args, conf, and extractor impl class
    new EsurientEtlMetadataManager(args, conf, extractor)
  }

}
