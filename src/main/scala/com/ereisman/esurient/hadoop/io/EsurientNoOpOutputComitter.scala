package com.ereisman.esurient.hadoop.io

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.{JobContext,TaskAttemptContext}
import org.apache.hadoop.mapreduce.OutputCommitter
import java.io.{DataInput,DataOutput}

/**
 * Dummy class to fake out Hadoop so we can ask for a
 * fixed number of Mapper tasks to run our ETL code in.
 *
 * This OutputCommitter is a NO OP it is only for times
 * (like testing) that we don't want any output at all.
 * "real" jobs might use Hadoop's FileOutputCommitter etc.
 */
class EsurientNoOpOutputComitter extends OutputCommitter {

  override def abortTask(tac: TaskAttemptContext): Unit = { }

  override def commitTask(tac: TaskAttemptContext): Unit = { }

  override def needsTaskCommit(tac: TaskAttemptContext) = false

  override def setupJob(jc: JobContext): Unit = { }

  override def setupTask(tac: TaskAttemptContext): Unit = { }

  override def commitJob(jc: JobContext): Unit = { }
}

