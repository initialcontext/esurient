package com.ereisman.esurient.util


import org.apache.log4j.Logger

import com.ereisman.esurient.EsurientConstants._
import com.ereisman.esurient.EsurientTask


object EsurientStats {
  val LOG = Logger.getLogger(classOf[EsurientStats])
}


/**
 * Very rudimentary class to ping monitoring service. The message format:
 * <code>dot.separated.metrics.basename.%s.task%s %s</code>
 * where args are: 'job name or equiv metric subkey', 'taskId' and 'integer metric value'
 *
 * TODO: generalize this into a nice class hierarchy for all sorts of metrics and fun.
 */
class EsurientStats(val context: EsurientTask.Context) {
  import com.ereisman.esurient.util.EsurientStats.LOG
  // conditionally configure heartbeat logging to also ping monitoring service (rudimentary as of now)
  val (
    host: Option[String],
    port: Option[String],
    metricsKey: Option[String],
    taskId: Option[String]
  ) = initializeMetrics


  /**
   * Ping a metrics endpoint with a user-supplied String formatting key and
   * an integral value. Idiot simple for now to interact with a particular framework.
   */
  def pingMetrics(msgFormat: String, value: String): Unit = {
    synchronized {
      host.foreach { h =>
        val timeStamp = System.currentTimeMillis / 1000L
        val formattedMsg = msgFormat.format(metricsKey.get, taskId.get, value)  + " " + timeStamp
        val echoStmt = "echo " + formattedMsg
        val ncStmt = "/usr/bin/nc " + h + " " + port.get
        statsShellOut(echoStmt, ncStmt)
      }
    }
  }


  def initializeMetrics:
    (Option[String], Option[String], Option[String], Option[String]) = {
    context.getConfiguration.get(ES_METRICS_HOST_PORT, "") match {
      case hostPort: String if (hostPort.length > 0) => {
        val hp = hostPort.split(":")
        (Some(hp(0)), Some(hp(1)), Some(getMetricsKey), Some(getTaskId))
      }
      // this ensures none of the host.foreach() calls will do anything during pingMonitoring() calls
      case _                => (None, None, None, None)
    }
  }


  private def statsShellOut(cmd1: String, cmd2: String): Unit = {
    import sys.process._
    val exitCode = cmd1 #| cmd2 !

    exitCode match {
      case 0          => Unit // do nothing
      case eCode: Int =>
        LOG.warn("Monitoring ping to " + host.get + " failed with exit code: " + eCode)
    }
  }
  

  private def getTaskId: String =
    context.getConfiguration.getInt(ES_THIS_TASK_ID, ES_ERROR_CODE).toString


  private def getMetricsKey: String =
    context.getConfiguration.get(ES_METRICS_KEY, ES_JOB_NAME.toLowerCase.replaceAll("[^A-Za-z0-9]+", "_"))
}
