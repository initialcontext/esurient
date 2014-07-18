package com.ereisman.esurient.util


import com.ereisman.esurient.EsurientConstants._

import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration

import java.io.{IOException,PrintWriter}
import java.net.Socket


object PushableMetric {
  val LOG = Logger.getLogger(classOf[PushableMetric])
}

/**
 * Very rudimentary class to ping monitoring service. The message format:
 * <code>dot.separated.metrics.basename.%s.task%s %s</code>
 * where args are: 'job name or equiv metric subkey', 'taskId' and 'integer metric value'
 *
 * TODO: 1. Generalize this into a nice class hierarchy for all sorts of metrics
 *       2. Implement this class using sockets instead of shell out now that we're happy with this.
 */
class PushableMetric(val conf: Configuration, val params: Map[String, String]) extends Runnable {
  import com.ereisman.esurient.util.PushableMetric.LOG

  // conditionally configure heartbeat logging to also ping monitoring service (rudimentary as of now)
  val hostPort     = getHostPort
  val taskId       = conf.getInt(ES_THIS_TASK_ID, ES_ERROR_CODE).toString
  val metricKey    = conf.get(ES_METRICS_KEY, ES_JOB_NAME.toLowerCase.replaceAll("[^A-Za-z0-9]+", "_"))
  val msgFormat    = params("msgFormat")
  val metricValue  = params("metricValue")
  val formattedMsg = msgFormat.format(metricKey, taskId, metricValue)  + " " + params("timeStamp")


  override def run: Unit = {
    hostPort.foreach { hp =>
      var socket: Socket = null
      var out: PrintWriter = null

      try {
        socket = getSocket(hp)
        out = new PrintWriter(socket.getOutputStream, true)
        out.println(formattedMsg)
      } catch {
        case t: Throwable =>
          LOG.warn("Push of metric '" + formattedMsg + "' failed: " + t.toString + ": " + t.getMessage)
      } finally {
        if (out != null) try { out.close } catch { case _ => /* do nothing */ }
        if (socket != null) try { socket.close } catch { case _ => /* do nothing */ }
      }
    }
  }


  private def getSocket(hp: (String, String)): Socket = {
    val socket = new Socket(hp._1, hp._2.toInt)
    socket.setKeepAlive(false)
    socket.setSoTimeout(ES_METRICS_SEND_TIMEOUT)
    socket
  }


  private def getHostPort: Option[(String, String)] = {
    conf.get(ES_METRICS_HOST_PORT, "") match {
      case result: String if (result.length > 0) => {
        val hp = result.split(":")
        Some( (hp(0), hp(1)) )
      }
      case _  => None
    }
  }
}
