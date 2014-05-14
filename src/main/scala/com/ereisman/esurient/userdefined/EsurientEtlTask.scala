package com.ereisman.esurient.userdefined


import org.apache.hadoop.conf.Configuration

import org.apache.log4j.Logger

import com.ereisman.esurient.db.{Database,DatabaseFactory}


object EsurientEtlTask {
  val LOG = Logger.getLogger(classOf[EsurientEtlTask])
  val ERROR = -1
}


class EsurientEtlTask extends com.ereisman.esurient.EsurientTask {

  override def execute: Unit = {
    val taskId = context.getConfiguration.getInt("esurient.this.task.id", EsurientEtlTask.ERROR)
    val totalTasks = context.getConfiguration.getInt("esurient.task.count", EsurientEtlTask.ERROR)
    val conf = context.getConfiguration
    val db = DatabaseFactory.getDatabase(taskId, conf)

    // TODO: set up DB metadata objects to track connections etc. before we run the task 

    // TODO: do work!!!
  }
}

