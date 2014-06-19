package com.ereisman.esurient.examples


import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger

import com.ereisman.esurient.EsurientTask
import com.ereisman.esurient.etl.format.EtlOutputFormatterFactory


/**
 * Executable host container for ETL task, runs the Driver, passing it
 * Hadoop Configuration containing all important metadata from the job
 * properties file hosted on HDFS and generated beforehand by a run of
 * the EsurientEtlMetadataManager.
 */
class EsurientEtlTask extends EsurientTask {

  override def execute: Unit = {
    // this is needed to configure the job
    val jobConfig = context.getConfiguration
    // this is pluggable - write your own output class (see etl.format package)
    val formatter = EtlOutputFormatterFactory.getFormatter(jobConfig)

    // execute the ETL job
    new com.ereisman.esurient.etl.EsurientEtlDriver(jobConfig, formatter)
  }  

}
