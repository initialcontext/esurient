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
    // the Hadoop Configuration for this job full of useful metadata, and user-set job properties
    val conf = context.getConfiguration

    // the unique task id you can use to assign work deterministically to each task
    val taskId = conf.getInt(ES_THIS_TASK_ID, ERROR)
    // use modular arithmetic and the monotonic integers assigned as 
    // taskId's to partition the tasks in this job into various work groups
    val groupId = taskId % conf.getInt(ES_TASK_GROUPS, 4)
    // total # of tasks launched in this job, in this case set by the esurient-example-job.properties file
    val totalTasks = conf.getInt(ES_TASK_COUNT, ERROR)
    // retrieve another user-defined value set in properties file, injected into Configuration,
    // that has no meaning outside the user's application class:
    val myUserDefinedValue = conf.getInt("user.job.arg.1", 0)
    // same, but this time its a String value
    val myOtherUserDefinedValue = conf.get("can.be.called.anything", "error, no value set?")

    // do some hello world stuff to demonstrate using Configuration values
    // to assign task and group work deterministically (if desired)
    LOG.info("Hi, This is EsurientDefaultTask #" + taskId + " of " + totalTasks)
    
    // partition tasks up into work groups by their task ID.
    LOG.info("Task #" + taskId + "is a member of work group #" + groupId)
    LOG.info( groupId match {
      case 0 => "My job is to process files A-F"
      case 1 => "My job is to process files G-K"
      case 2 => "My job is to process files L-P"
      case 3 => "My job is to process files Q-Z"
    })

    // these are the random user defined values from the job properties file
    LOG.info("user.job.arg.1 from example properties file has value: " + myUserDefinedValue)
    LOG.info("can.be.called.anything from example properties file has value: " + myOtherUserDefinedValue)
    
    // lets make task #7 (if there is one) do something specific, act as a "master" node, whatever
    if (taskId == 7) {
      LOG.warn("You better watch out, I'm Task ID #7")
    }
  }
}

