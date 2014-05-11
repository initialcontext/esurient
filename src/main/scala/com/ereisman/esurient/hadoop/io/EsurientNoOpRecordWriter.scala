package com.ereisman.esurient.hadoop.io


import org.apache.hadoop.mapreduce.{RecordWriter,TaskAttemptContext}
import org.apache.hadoop.io.NullWritable


/**
 * Dummy class to fake out Hadoop so we can ask for a set # of Mapper tasks
 * to run our ETL jobs in. There will be no Reduce phase to this job, and no
 * actual "MapReduce" processing happening at all.
 */
class EsurientNoOpRecordWriter extends RecordWriter[NullWritable, NullWritable] {

  override def close(tac: TaskAttemptContext): Unit = { }

  override def write(key: NullWritable, value: NullWritable): Unit = {
    throw new IllegalStateException("ERROR: EsurientNoOpRecordWriter should _never_ be written to!")
  }
}

