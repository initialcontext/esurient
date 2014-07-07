package com.ereisman.esurient.etl.format


import org.apache.hadoop.conf.Configuration
import com.ereisman.esurient.EsurientConstants._


object EtlOutputFormatterFactory {
  def getFormatter(conf: Configuration): EtlOutputFormatter = {
    conf.get(ES_DB_OUTPUT_FORMAT, ES_DB_OUTPUT_FORMAT_TSV) match {
      case ES_DB_OUTPUT_FORMAT_TSV    => new TsvEtlOutputFormatter(conf)
      case _                          => throw new RuntimeException(
        "Only TSV output format is currently implemented, please set Configuration key '" +
        conf.get(ES_DB_OUTPUT_FORMAT, "ERROR_NOT_SET") + "' to value '" +
        ES_DB_OUTPUT_FORMAT_TSV + "' and rerun."
      )
    }
  }
}
