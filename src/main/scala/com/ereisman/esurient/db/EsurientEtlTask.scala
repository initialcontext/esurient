package com.ereisman.esurient.db


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.util.Progressable

import org.apache.log4j.Logger


import com.ereisman.esurient.hadoop.io.EsurientInputSplit
import com.ereisman.esurient.db.{DatabaseFactory,Database}
import com.ereisman.esurient.EsurientTask


object EsurientTask {
  val LOG = Logger.getLogger(classOf[EsurientTask])
}


class EsurientEtlTask(context: Mapper[NullWritable, NullWritable, NullWritable, NullWritable]#Context) extends EsurientTask {
  val taskId = context.getInputSplit.asInstanceOf[EsurientInputSplit].splitId
  val totalTasks = context.getInputSplit.asInstanceOf[EsurientInputSplit].numSplits
  val conf = context.getConfiguration
  val db = DatabaseFactory.getDatabase(taskId, conf)
  // TODO: set up DB metadata objects to track connections etc. before we run the task


  override def execute: Unit = { /* TODO */ }

  override def progress: Unit = { context.progress }
}

