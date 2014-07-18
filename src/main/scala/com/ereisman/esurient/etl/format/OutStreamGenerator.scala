package com.ereisman.esurient.etl.format


import com.ereisman.esurient.EsurientConstants._

import java.io.{OutputStream,BufferedOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path,FileSystem}
import org.apache.hadoop.io.compress.{CompressionCodec,CompressionCodecFactory}
import org.apache.log4j.Logger


object OutStreamGenerator {
  val LOG = Logger.getLogger(classOf[OutStreamGenerator])
}


class OutStreamGenerator(conf: Configuration) {
  import com.ereisman.esurient.etl.format.OutStreamGenerator.LOG


  def codecClassName: String = conf.get(ES_DB_OUTPUT_COMP_TYPE, ES_DB_OUTPUT_COMP_TYPE_DEFAULT) 


  // helper for output file naming
  def getFileTypeSuffix = ""


  def getOutputStream(fs: FileSystem, outputPath: Path): OutputStream = {
    val bos = new BufferedOutputStream(fs.create(outputPath, true), BUFFER_SIZE)
    (new CompressionCodecFactory(conf)).getCodecByClassName(codecClassName) match {
      case cc: CompressionCodec => cc.createOutputStream(bos)
      case _                   => {
        LOG.warn("Could not apply desired compression to output stream: " + codecClassName +
          " - check your Hadoop site.xml configurations for available codecs.")
        bos
      }
    }
  }
}
