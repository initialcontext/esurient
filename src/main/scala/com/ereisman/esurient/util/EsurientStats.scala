package com.ereisman.esurient.util


import org.apache.log4j.Logger

import com.ereisman.esurient.EsurientTask

import java.util.concurrent.{Executors,ExecutorService}


object EsurientStats {
  val LOG = Logger.getLogger(classOf[EsurientStats])
}


/**
 * Manages a queue of outgoing metrics messages, pushes the messages out
 * in serial via worker thread.
 *
 * TODO: make nice class hierarchy of metrics types and generalize the metadata
 *       delivered in the Configuration more too.
 */
class EsurientStats(val context: EsurientTask.Context) {
  import com.ereisman.esurient.util.EsurientStats.LOG
  // all calls to ping metrics services should be placed in thread queue
  val threadQueue: ExecutorService = Executors.newSingleThreadExecutor


  /**
   *  Call this with some key-value params that your impl class knows what to do with.
   *  Generic metadata the metric service needs to do its job are provided by the Configuration.
   */
  def pushMetric(params: Map[String, String]): Unit = {
    synchronized {
      threadQueue.submit(new PushableMetric(context.getConfiguration, params))
    }
  }
}
