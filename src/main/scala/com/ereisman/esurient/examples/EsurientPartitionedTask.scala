package com.ereisman.esurient.examples


import org.apache.hadoop.conf.Configuration

import org.apache.log4j.Logger

import com.ereisman.esurient.EsurientConstants._


object EsurientPartitionedTask {
  val LOG = Logger.getLogger(this.getClass())
  val ES_ERROR_CODE = -1
}


class EsurientPartitionedTask extends com.ereisman.esurient.EsurientTask {
  import com.ereisman.esurient.examples.EsurientPartitionedTask._

  override def execute: Unit = {
    // the Hadoop Configuration for this job. Populated with useful metadata,
    // user-defined job properties, and a unique TaskId. You can use the
    // Configuration to obtain a FileSystem object (to access HDFS)
    // and much more (see Hadoop documentation)
    val conf = context.getConfiguration

    // use the Hadoop Configuration to obtain the unique task ID the framework
    // has assigned this process. You can use it to assign work deterministically.
    val taskId = conf.getInt(ES_THIS_TASK_ID, ES_ERROR_CODE)
    // can use modular arithmetic and the monotonic integers assigned as 
    // taskId's to partition the tasks in this job into various work groups
    val groupId = taskId % conf.getInt(ES_TASK_GROUPS, 4)
    // total # of tasks launched in this job, in this case set by the esurient-example-job.properties file
    val totalTasks = conf.getInt(ES_TASK_COUNT, ES_ERROR_CODE)

    // retrieve another user-defined value set in properties file, injected into Configuration,
    // that has no meaning outside the user's application class:
    val myUserDefinedValueOne = conf.getInt("esurient.user.job.arg.1", ES_ERROR_CODE)
    val myUserDefinedValueTwo = conf.get("esurient.user.job.arg.2", "ERROR no value set")
    // same, but this time its a String value
    val myOtherUserDefinedValue = conf.get("can.be.called.anything", "ERROR no value set")

    // do some hello world stuff to demonstrate using Configuration values
    // to assign task and group work deterministically (if desired)
    LOG.info("Hi, This is EsurientPartitionedTask (" + taskId + " of " + totalTasks + ")")    
    // partition tasks up into work groups by their task ID.
    LOG.info("Task (" + taskId + ") is a member of work group (" + groupId + ")")

    // these are some user defined values from the example job properties file
    LOG.info("esurient.user.job.arg.1 from example properties file has value: " + myUserDefinedValueOne)
    LOG.info("esurient.user.job.arg.2 from example properties file has value: " + myUserDefinedValueTwo)
    LOG.info("can.be.called.anything from example properties file has value: " + myOtherUserDefinedValue)
    
    // lets make task #7 (if there is one) do something specific, act as a "master" node, whatever
    if (taskId == 7) {
      LOG.warn("You better watch out, I'm Task ID #7")
    }
  }
}

