package com.ereisman.esurient.etl.format


import com.ereisman.esurient.EsurientConstants._

import org.apache.hadoop.conf.Configuration


/**
 * Implementation plugin for gzip compression on output files
 */
class GzipOutStream(conf: Configuration) extends OutStreamGenerator(conf) { 

  override def getFileTypeSuffix: String = ".gz"

  override def codecClassName: String = ES_DB_OUTPUT_COMP_TYPE_DEFAULT
}

