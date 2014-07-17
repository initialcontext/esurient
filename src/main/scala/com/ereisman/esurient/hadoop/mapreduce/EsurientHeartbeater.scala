package com.ereisman.esurient.hadoop.mapreduce


import com.ereisman.esurient.EsurientTask
import com.ereisman.esurient.EsurientConstants._
import com.ereisman.esurient.util.EsurientStats

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.util.Progressable
import org.apache.log4j.Logger


/**
 * Call progress periodically on a timer to keep the Mapper task active
 * unless an exception is propagated all the way back to the top level or
 * the job completes some other way.
 */
object EsurientHeartbeater {
  val LOG = Logger.getLogger(classOf[EsurientHeartbeater])
  val MB_SIZE = 1024 * 1024
  val DATE_FORMATTER = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
}


/**
 * @param context metadata about the task and the underlying Hadoop job.
 *
 * @param done allows the parent process (the job) to ensure this heartbeat
 *             thread is eliminated if it throws an exception or completes successfully.
 */
class EsurientHeartbeater(context: EsurientTask.Context, stats: EsurientStats, done: AtomicBoolean) extends java.lang.Thread {
  import com.ereisman.esurient.hadoop.mapreduce.EsurientHeartbeater._
  val rt = Runtime.getRuntime
  // see if the user wants us to log the heartbeats
  val logHeartBeats = context.getConfiguration.getBoolean(ES_LOG_HEARTBEATS, false)
  // see if the user has an opinion about how often to send heartbeats
  val heartbeatMillis = context.getConfiguration
    .getLong(ES_TASK_AUTO_HEARTBEAT_MILLIS, ES_TASK_AUTO_HEARTBEAT_MILLIS_DEFAULT)
  // heartbeat metrics msg formating string, set by default or w/value from job.properties file
  val metricsFormatStr = context.getConfiguration
    .get(ES_HEARTBEAT_METRICS_MSG, ES_HEARTBEAT_METRICS_MSG_DEFAULT)


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

    LOG.info("HEARTBEAT at " + DATE_FORMATTER.format(new java.util.Date) +
      " | Heap Size: Used(" + used + " MB) Free(" + free + " MB)")

    // only pings stats endpoint if host:port were provided in job.properties file 
    stats.pingMetrics(metricsFormatStr, used.toString)
  }
}

