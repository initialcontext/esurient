package com.ereisman.esurient.hadoop.mapreduce


import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.io.NullWritable

import com.ereisman.esurient.hadoop.io.EsurientInputSplit
import com.ereisman.esurient.EsurientTask


/**
 * A Hadoop Mapper subclass to act as a wrapper for the ETL process we will execute.
 */
class EsurientMapper extends Mapper[NullWritable, NullWritable, NullWritable, NullWritable] {
  type MapCtx = Mapper[NullWritable, NullWritable, NullWritable, NullWritable]#Context

  override def map(dummyK: NullWritable, dummyV: NullWritable, context: MapCtx): Unit = { new EsurientTask(context) }
}

