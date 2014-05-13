package com.ereisman.esurient.db


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable

import org.apache.log4j.Logger

import com.ereisman.esurient.hadoop.io.EsurientInputSplit


object EsurientEtlTask {
  val LOG = Logger.getLogger(classOf[EsurientEtlTask])
}


class EsurientEtlTask extends com.ereisman.esurient.EsurientTask {

  override def initialize: Unit = {
    val taskId = context.getInputSplit.asInstanceOf[EsurientInputSplit].splitId
    val totalTasks = context.getInputSplit.asInstanceOf[EsurientInputSplit].numSplits
    val conf = context.getConfiguration
    val db = DatabaseFactory.getDatabase(taskId, conf)
    // TODO: set up DB metadata objects to track connections etc. before we run the task
  }

  override def execute: Unit = { /* TODO */ }
}

