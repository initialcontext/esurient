package com.ereisman.esurient.hadoop.io


import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{InputSplit,TaskAttemptContext,RecordReader}


/**
 * Dummy class to fake out Hadoop so we can ask for a set # of Mapper tasks
 * to run our ETL jobs in. There will be no Reduce phase to this job, and no
 * actual "MapReduce" processing happening at all.
 *
 * The key to the trick is returning 1 record from each of these object instantiated
 * so that the Mapper recieving it will believe it got input, and will execute.
 */

object EsurientNoOpRecordReader {
  val Key = NullWritable.get
  val Value = NullWritable.get
}

class EsurientNoOpRecordReader extends RecordReader[NullWritable, NullWritable] {
  var recordSeenOnce = false

  override def close: Unit = { }

  override def getProgress: Float = recordSeenOnce match {
    case false => 0.0f
    case _ => 1.0f
  }

  override def getCurrentKey = EsurientNoOpRecordReader.Key
  override def getCurrentValue = EsurientNoOpRecordReader.Value

  override def initialize(split: InputSplit, tac: TaskAttemptContext): Unit = { }

  override def nextKeyValue = recordSeenOnce match {
    case false => recordSeenOnce = true ; true
    case _ => false
  }
}

