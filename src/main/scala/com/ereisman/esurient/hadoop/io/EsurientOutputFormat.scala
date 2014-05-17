package com.ereisman.esurient.hadoop.io


import org.apache.hadoop.mapreduce.{JobContext,OutputFormat,RecordWriter,TaskAttemptContext,OutputCommitter}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.Path


/**
 * A dummy class used to fake out Hadoop so that we can run our
 * ETL tasks in Mappers on the cluster.
 */
class EsurientOutputFormat extends OutputFormat[NullWritable, NullWritable] {

  override def checkOutputSpecs(jc: JobContext): Unit = { }

  // TODO: check conf fields and optionally change the committer to something more useful!
  override def getOutputCommitter(tac: TaskAttemptContext): OutputCommitter = {
    new EsurientNoOpOutputCommitter
  }

  override def getRecordWriter(tac: TaskAttemptContext): RecordWriter[NullWritable, NullWritable] = {
    new EsurientNoOpRecordWriter
  }
}

