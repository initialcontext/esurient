package com.ereisman.esurient.userdefined


import org.apache.hadoop.conf.Configuration

import org.apache.log4j.Logger

import com.ereisman.esurient.EsurientConstants._


object EsurientPartitionedTask {
  val LOG = Logger.getLogger(this.getClass())
  val ERROR = -1
}


class EsurientPartitionedTask extends com.ereisman.esurient.EsurientTask {
  import com.ereisman.esurient.userdefined.EsurientPartitionedTask._

  override def execute: Unit = {
    // the unique task id you can use to assign work deterministically to each task
    val taskId = context.getConfiguration.getInt(ES_THIS_TASK_ID, ERROR)
    // use modular arithmetic and the monotonic integers assigned as 
    // taskId's to partition the tasks in this job into various work groups
    val groupId = taskId % context.getConfiguration.getInt(ES_TASK_GROUPS, 4)
    // the total # of tasks launched in this job
    val totalTasks = context.getConfiguration.getInt(ES_TASK_COUNT, ERROR)
    // the Hadoop Configuration for this job full of useful metadata, values from cmd line args etc.
    val conf = context.getConfiguration

    // do some hello world stuff and bail out
    LOG.info("Hi, This is EsurientDefaultTask #" + taskId + " of " + totalTasks)
    LOG.info("Task #" + taskId + "is a member of work group #" + groupId)
    LOG.info( groupId match {
      case 0 => "My job is to process files A-F"
      case 1 => "My job is to process files G-K"
      case 2 => "My job is to process files L-P"
      case 3 => "My job is to process files Q-Z"
    })
    if (taskId == 7) {
      LOG.warn("You better watch out, I'm Task ID #7")
    }
  }
}

