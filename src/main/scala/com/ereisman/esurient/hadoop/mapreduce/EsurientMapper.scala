package com.ereisman.esurient.hadoop.mapreduce


import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.io.NullWritable

import com.ereisman.esurient.hadoop.io.EsurientInputSplit
import com.ereisman.esurient.EsurientTask
import com.ereisman.esurient.EsurientEtlTask

/**
 * A Hadoop Mapper subclass to act as a wrapper for the ETL process we will execute.
 */
class EsurientMapper extends Mapper[NullWritable, NullWritable, NullWritable, NullWritable] {
  type MapCtx = Mapper[NullWritable, NullWritable, NullWritable, NullWritable]#Context
  // the Task that will be executed by the Esurient framework
  var esurientTask: EsurientTask = null

  override def run(context: MapCtx): Unit = {
    try {
      setup(context)
      if (context.nextKeyValue()) esurientTask.execute
    } catch (Exception propagatedUp) {
      throw propagatedUp
    } finally {
      cleanup(context)
    } 
  }

  /**
   * Execute a user defined (or generic EsurientTask) job that takes the Hadoop
   * Configuration object and pulls all arguments and task metadata from it,
   * including the task's own unique ID number, obtained from the task's
   * EsurientInputSplit which the framework rigs for you ;)
   *
   * @param context the Mapper#Context for this job
   */
  override def setup(context: MapCtx): Unit = {
    injectTaskIdentityMetadata(context)
    esurientTask = conf.get("esurient.task.class.name") match {
      case klazz: String => instantiateUserDefinedTask(context)
      case _             => new EsurientEtlTask(context)
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
  def instantiateUserDefinedTask(context: MapCtx): EsurientTask = {
    val clsName = context.getConfiguration("esurient.this.task.id")
    Class.forName(clsName).getConstructor(Array[Class](classOf[Configuration]))(context)
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
  def injectTaskIdentityMetadata(context: MapCtx): Unit = {
    val taskId = context.getInputSplit.asInstanceOf[EsurientInputSplit].splitId
    val taskCount = context.getInputSplit.asInstanceOf[EsurientInputSplit].numSplits
    context.getConfiguration.set("esurient.this.task.id", taskId)
    context.getConfiguration.set("esurient.task.count", taskCount)
  }

  override def map(dummyK: NullWritable, dummyV: NullWritable, context: MapCtx): Unit = { /* no-op */ }
  override def cleanup(context: MapCtx): Unit = { /* do some Hadoop-side cleanup here if needed */ }
}
