package com.ereisman.esurient.userdefined


import org.apache.hadoop.conf.Configuration

import org.apache.log4j.Logger


object EsurientDefaultTask {
  val LOG = Logger.getLogger(this.getClass())
  val ERROR = -1
}


class EsurientDefaultTask extends com.ereisman.esurient.EsurientTask {
  import com.ereisman.esurient.userdefined.EsurientDefaultTask._

  override def execute: Unit = {
    // the unique task id you can use to assign work deterministically to each task
    val taskId = context.getConfiguration.getInt("esurient.this.task.id", ERROR)
    // the total # of tasks launched in this job
    val totalTasks = context.getConfiguration.getInt("esurient.task.count", ERROR)
    // the Hadoop Configuration for this job full of useful metadata, values from cmd line args etc.
    val conf = context.getConfiguration

    // do some hello world stuff and bail out
    LOG.info("Hi, This is EsurientDefaultTask #" + taskId + " of " + totalTasks)
    if (taskId == 7) {
      LOG.warn("You better watch out, I'm Task ID #7")
    }
  }
}

