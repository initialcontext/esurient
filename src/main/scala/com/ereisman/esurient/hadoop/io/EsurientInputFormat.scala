package com.ereisman.esurient.hadoop.io


import org.apache.hadoop.mapreduce.{JobContext,RecordReader,InputFormat,InputSplit,TaskAttemptContext}
import org.apache.hadoop.io.NullWritable

import com.ereisman.esurient.EsurientConstants._


/**
 * Dummy class who's main purpose is to fake out Hadoop and trick
 * it into giving us 'numTasks' # of Mappers to run our ETL code in.
 */
class EsurientInputFormat extends InputFormat[NullWritable, NullWritable] {

  override def getSplits(jc: JobContext): java.util.List[InputSplit] = {
    val numTasks = jc.getConfiguration.getInt(ES_TASK_COUNT, ES_TASK_COUNT_DEFAULT)

    // return a Java list of numTasks worth of dummy InputSplits to fool Hadoop
    (1 to numTasks).foldLeft(new java.util.LinkedList[InputSplit]) {
      (acc, i) => acc.add(new EsurientInputSplit(i, numTasks)) ; acc
    }
  }


  override def createRecordReader(split: InputSplit, tac: TaskAttemptContext): RecordReader[NullWritable, NullWritable] = {
    new EsurientNoOpRecordReader
  }
}

