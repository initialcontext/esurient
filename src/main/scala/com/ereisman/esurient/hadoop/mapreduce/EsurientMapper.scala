package com.ereisman.esurient.hadoop.mapreduce


import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.io.NullWritable

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.log4j.Logger

import com.ereisman.esurient.hadoop.io.EsurientInputSplit
import com.ereisman.esurient.EsurientTask
import com.ereisman.esurient.EsurientConstants._
// until we publish artifact, put user-defined task classes in here and compile together
import com.ereisman.esurient.examples._


object EsurientMapper {
  val LOG = Logger.getLogger(classOf[EsurientMapper])
}

/**
 * A Hadoop Mapper subclass to act as a wrapper for the ETL process we will execute.
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
    checkAutoHeartbeatSelected(context)
    esurientTask = context.getConfiguration.get(ES_TASK_CLASS_NAME) match {
      case klazz: String => instantiateUserDefinedTask(context)
      case _             => (new EsurientDefaultTask).init(context)
    }
  }

  /**
   * If the user set the Configuration key (via cmd line args or site.xml file)
   * then we can auto-heartbeat at a fixed interval in a background thread.
   */
  def checkAutoHeartbeatSelected(context: EsurientTask.Context): Unit = {
    context.getConfiguration.getBoolean(ES_TASK_AUTO_HEARTBEAT, true) match {
      case true => new EsurientAutomaticHeartbeater(context, done).start
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
  def instantiateUserDefinedTask(context: EsurientTask.Context): EsurientTask = {
    val clsName = context.getConfiguration.get(ES_TASK_CLASS_NAME)
    Class.forName(clsName).newInstance.asInstanceOf[EsurientTask].init(context)
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


/**
 * Call progress periodically on a timer to keep the Mapper task active
 * unless an exception is propagated all the way back to the top level or
 * the job completes some other way.
 */
object EsurientAutomaticHeartbeater {
  val LOG = Logger.getLogger(classOf[EsurientAutomaticHeartbeater])
  val MB_SIZE = 1024 * 1024
  val formatter = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
}


/**
 * @param context metadata about the task and the underlying Hadoop job.
 *
 * @param done allows the parent process (the job) to ensure this heartbeat
 *             thread is eliminated if it throws an exception or completes successfully.
 */
class EsurientAutomaticHeartbeater(context: EsurientTask.Context, done: AtomicBoolean) extends java.lang.Thread {
  import com.ereisman.esurient.hadoop.mapreduce.EsurientAutomaticHeartbeater._
  val rt = Runtime.getRuntime
  // see if the user wants us to log the heartbeats
  val logHeartBeats = context.getConfiguration.getBoolean(ES_LOG_HEARTBEATS, false)
  // see if the user has an opinion about how often to send heartbeats
  val heartbeatMillis = context.getConfiguration
    .getLong(ES_TASK_AUTO_HEARTBEAT_MILLIS, ES_TASK_AUTO_HEARTBEAT_MILLIS_DEFAULT)
  // conditionally configure heartbeat logging to also ping monitoring service (rudimentary as of now)
  val (host: Option[String],
    port: Option[String],
    msgTemplate: Option[String],
    monitorKey: Option[String],
    taskId: Option[String]
  ) = initializeMonitoring


  override def run(): Unit = {
    LOG.info("Heartbeats will be issued automatically for this run at " + (heartbeatMillis/1000) + " second intervals")
    while (!done.get) {
      if (logHeartBeats) { logHeartBeat }
      context.progress
      java.lang.Thread.sleep(heartbeatMillis)
    }
  }


  private def logHeartBeat: Unit = {
    val used = (rt.totalMemory - rt.freeMemory) / MB_SIZE
    val free = rt.freeMemory / MB_SIZE
    LOG.info("HEARTBEAT at " + formatter.format(new java.util.Date) +
      " | Heap Size: Used(" + used + " MB) Free(" + free + " MB)")
    host.foreach { h: String => pingMonitoring( used.toString ) }
  }

  // idiot simple for now, just hit endpoint with key/value/timestamp msg
  // TODO: add this to Utils, inititialize but expose to EsurientTasks to push any stats
  private def pingMonitoring(value: String): Unit = {
    import sys.process._

    val timeStamp = System.currentTimeMillis / 1000L
    val formattedMsg = msgTemplate.get.format(monitorKey.get, taskId.get, value)  + " " + timeStamp
    val echoStmt = "echo " + formattedMsg
    val ncStmt = "/usr/bin/nc " + host.get + " " + port.get
    val exitCode = echoStmt #| ncStmt !

    exitCode match {
      case eCode: Int if (eCode != 0) =>
        LOG.warn("Monitoring ping to " + host.get + " failed with exit code: " + eCode)

      case _                          => Unit // do nothing, successful ping
    }
  }


  private def initializeMonitoring:
    (Option[String], Option[String], Option[String], Option[String], Option[String]) = {
    context.getConfiguration.get(ES_MONITORING_HOST_PORT, "") match {
      case hostPort: String if (hostPort.length > 0) => {
        val hp = hostPort.split(":")
        (Some(hp(0)), Some(hp(1)), Some(getMonitoringMessageTemplate), Some(getMonitoringKey), Some(getTaskId))
      }
      // this ensures none of the "host.foreach" calls will do anything during heartbeats
      case _                => (None, None, None, None, None)
    }
  }

  
  private def getTaskId: String =
    context.getConfiguration.getInt(ES_THIS_TASK_ID, ES_ERROR_CODE).toString


  private def getMonitoringKey: String =
    context.getConfiguration.get(ES_MONITORING_KEY, "ERROR_NO_KEY_SUPPLIED")


  private def getMonitoringMessageTemplate: String =
    context.getConfiguration.get(ES_MONITORING_MSG_TEMPLATE, ES_MONITORING_MSG_TEMPLATE_DEFAULT)
}

