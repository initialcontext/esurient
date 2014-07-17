package com.ereisman.esurient.hadoop.mapreduce


import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.io.NullWritable

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.log4j.Logger

import com.ereisman.esurient.hadoop.io.EsurientInputSplit
import com.ereisman.esurient.EsurientTask
import com.ereisman.esurient.util.EsurientStats
import com.ereisman.esurient.EsurientConstants._

import com.ereisman.esurient.examples._


object EsurientMapper {
  val LOG = Logger.getLogger(classOf[EsurientMapper])
}

/**
 * A Hadoop Mapper subclass to host an EsurientTask we will execute.
 */
class EsurientMapper extends Mapper[NullWritable, NullWritable, NullWritable, NullWritable] {
  import com.ereisman.esurient.hadoop.mapreduce.EsurientMapper._

  // the Task that will be executed by the Esurient framework
  var esurientTask: EsurientTask = null
  var done = new AtomicBoolean(false)


  // exceptions will (and should) propagate if they made it this far
  override def run(context: EsurientTask.Context): Unit = {
    try {
      setup(context)
      if (context.nextKeyValue()) esurientTask.execute
    } finally {
      done.set(true)
      java.lang.Thread.sleep(1)
      cleanup(context)
    }
  }


  /**
   * Execute a user defined (or generic EsurientTask) job that takes the Hadoop
   * Configuration object and pulls all arguments and task metadata from it,
   * including the task's framework-assigned unique ID number. If this is a user
   * defined job, the full name of the class to instantiate is also expected to
   * be stored in the Hadoop Configuration.
   *
   * @param context the Mapper#Context for this job
   */
  override def setup(context: EsurientTask.Context): Unit = {
    injectTaskIdentityMetadata(context)
    val esurientStats = new EsurientStats(context)
    checkAutoHeartbeatSelected(context, esurientStats)

    esurientTask = context.getConfiguration.get(ES_TASK_CLASS_NAME) match {
      case klazz: String => instantiateUserDefinedTask(context, esurientStats)
      case _             => (new EsurientDefaultTask).init(context, esurientStats)
    }
  }


  /**
   * If the user set the Configuration key (via cmd line args or site.xml file)
   * then we can auto-heartbeat at a fixed interval in a background thread.
   */
  private def checkAutoHeartbeatSelected(context: EsurientTask.Context, stats: EsurientStats): Unit = {
    context.getConfiguration.getBoolean(ES_TASK_AUTO_HEARTBEAT, true) match {
      case true => new EsurientHeartbeater(context, stats, done).start
      case _    => Unit
    }
  }


  /**
   * A User can define a class extending the EsurientTask abstract class that takes
   * a Hadoop Configuration as its only constructor argument, and the framework will
   * run it as a generic task with a unique ID rather than running EsurientEtlTask.
   *
   * @param context the job context, including a Hadoop Configuration that will
   *                provide unique task id and task metadata.
   */
  private def instantiateUserDefinedTask(context: EsurientTask.Context, stats: EsurientStats): EsurientTask = {
    val clsName = context.getConfiguration.get(ES_TASK_CLASS_NAME)
    Class.forName(clsName).newInstance.asInstanceOf[EsurientTask].init(context, stats)
  }


  /**
   * Makes sure each task created gets its own unique identity number from the
   * EsurientInputSplits we used to fake out Hadoop. We provide these Configuration
   * keys just for user convenience. All other Configuration data will be identical
   * among all EsurientMappers.
   *
   * @param context the Hadoop Mapper#Context which wraps the Hadoop Configuration
   *                we want to inject the identity information into.
   */
  def injectTaskIdentityMetadata(context: EsurientTask.Context): Unit = {
    val taskId = context.getInputSplit.asInstanceOf[EsurientInputSplit].splitId
    val taskCount = context.getInputSplit.asInstanceOf[EsurientInputSplit].numSplits
    context.getConfiguration.setInt(ES_THIS_TASK_ID, taskId)
    context.getConfiguration.setInt(ES_TASK_COUNT, taskCount)
  }


  override def map(dummyK: NullWritable, dummyV: NullWritable, context: EsurientTask.Context): Unit = { /* no-op */ }

  override def cleanup(context: EsurientTask.Context): Unit = { /* do some Hadoop-side cleanup here if needed */ }
}

