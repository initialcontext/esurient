package com.ereisman.esurient.examples


import org.apache.hadoop.conf.Configuration

import org.apache.log4j.Logger

import com.ereisman.esurient.EsurientConstants._


object EsurientDefaultTask {
  val LOG = Logger.getLogger(this.getClass())
}


class EsurientDefaultTask extends com.ereisman.esurient.EsurientTask {
  import com.ereisman.esurient.examples.EsurientDefaultTask._

  override def execute: Unit = {
    // the unique task id you can use to assign work deterministically to each task
    val taskId = context.getConfiguration.getInt(ES_THIS_TASK_ID, ES_ERROR_CODE)
    // the total # of tasks launched in this job
    val totalTasks = context.getConfiguration.getInt(ES_TASK_COUNT, ES_ERROR_CODE)
    // the Hadoop Configuration for this job full of useful metadata, values from cmd line args etc.
    val conf = context.getConfiguration

    // do some hello world stuff and bail out
    LOG.info("Hi, This is EsurientDefaultTask #" + taskId + " of " + totalTasks)
    if (taskId == 7) {
      LOG.warn("You better watch out, I'm Task ID #7")
    }
  }
}

