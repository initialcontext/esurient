package com.ereisman.esurient


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.util.Progressable

import org.apache.log4j.Logger


import com.ereisman.esurient.hadoop.io.EsurientInputSplit
import com.ereisman.esurient.db.{DatabaseFactory,Database}


object EsurientTask {
  val LOG = Logger.getLogger(classOf[EsurientTask])
}


class EsurientTask(context: Mapper[NullWritable, NullWritable, NullWritable, NullWritable]#Context) {
  val taskId = context.getInputSplit.asInstanceOf[EsurientInputSplit].splitId
  val totalTasks = context.getInputSplit.asInstanceOf[EsurientInputSplit].numSplits
  val conf = context.getConfiguration
  // TODO: set up DB metadata objects to track connections etc. before we run the task
  val db = DatabaseFactory.getDatabase(taskId, conf)
  runTask


  def runTask: Unit = { /* TODO */ }

  // TODO: call context#progress() a lot when looping, doing long queries/calcs, or in bkgnd thread
  def progress: Unit = { context.progress }
}

