package com.ereisman.esurient.examples


import org.apache.hadoop.conf.Configuration

import org.apache.log4j.Logger

import com.ereisman.esurient.db.{Database,DatabaseFactory}
import com.ereisman.esurient.EsurientConstants._


object EsurientEtlTask {
  val LOG = Logger.getLogger(classOf[EsurientEtlTask])
  val ERROR = -1
}


class EsurientEtlTask extends com.ereisman.esurient.EsurientTask {
  import com.ereisman.esurient.examples.EsurientEtlTask._

  override def execute: Unit = {
    val taskId = context.getConfiguration.getInt(ES_THIS_TASK_ID, ERROR)
    val totalTasks = context.getConfiguration.getInt(ES_TASK_COUNT, ERROR)
    val conf = context.getConfiguration
    val db = DatabaseFactory.getDatabase(taskId, conf)

    // TODO: set up DB metadata objects to track connections etc. before we run the task 

    // TODO: do work!!!
  }
}
