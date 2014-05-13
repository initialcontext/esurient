package com.ereisman.esurient

object EsurientTask {
 type EsurientContext = org.apache.hadoop.mapreduce.Mapper[NullWritable, NullWritable, NullWritable, NullWritable]#Context
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
abstract class EsurientTask(val context: EsurientTask.EsurientContext) {

  def execute: Unit

  def progress = context.progress
}
