package com.ereisman.esurient.etl.format


import com.ereisman.esurient.EsurientConstants._

import org.apache.hadoop.conf.Configuration


/**
 * Implementation plugin for gzip compression on output files
 */
class GzipOutStream(conf: Configuration) extends OutStreamGenerator(conf) { 

  // 256MB in bytes, or ~ 2 HDFS blocks uncompressed,
  // since GZIP is not splittable by Hadoop jobs :(
  override def getMaxChunkSize: Long = (1024 * 1024 * 256)

  override def getFileTypeSuffix: String = ".gz"

  override def codecClassName: String = ES_DB_OUTPUT_COMP_TYPE_DEFAULT
}

