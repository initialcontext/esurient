package com.ereisman.esurient


import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Mapper

import com.ereisman.esurient.util.EsurientStats


object EsurientTask {
 // lets hide this ugly thing
 type Context = Mapper[NullWritable, NullWritable, NullWritable, NullWritable]#Context
}


/**
 * Users should subclass this to define their own generic cluster tasks.
 * The context contains (along with all sorts of Hadoop-level stuff) a
 * Configuration object which has been populated with all relevant task data
 * including this task's unique ID number.
 *
 * Like any Hadoop-based task, the user code _must_ remember to call progress()
 * often between any compute heavy or blocking operations to signal task health.
 */
abstract class EsurientTask {
  // horrible hack because Scala 2.9.x reflection is not really a thing
  final var context: EsurientTask.Context = null
  var       stats:   EsurientStats        = null


  // this should _only_ ever be called by the framework
  final def init(ctx: EsurientTask.Context, sts: EsurientStats): EsurientTask = {
    context = ctx // save this for subclasses to access
    stats = sts
    this // return 'this' for the EsurientMapper (framework) to hold on to
  }


  /**
   * Execute is called by the framework when we're ready to do stuff.
   *
   * An EsurientTask.Context (inlcuding the Hadoop job context, plus Hadoop Configuration,
   * plus any user task-specific metadata your task needs to run, plus unique task ID)
   * will be available as 'context' for you to extract setup data from.
   */
  def execute: Unit


  /**
   * Users should call progress frequently in between any long computations or blocking calls.
   * Signals task health back to the Hadoop cluster.
   */
  final def progress = if (context != null) context.progress
}
