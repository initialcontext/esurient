package com.ereisman.esurient.hadoop.io

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.InputSplit

import java.io.{DataInput,DataOutput}

/**
 * Dummy class to fake out Hadoop so we can ask for a
 * fixed number of Mapper tasks to run our ETL code in.
 */
class EsurientInputSplit(var splitId: Int, var numSplits: Int) extends InputSplit with Writable {
  
  def this() = { this(-1, -1) }
  
  override def getLength = 0L

  override def getLocations = Array[String]()

  override def readFields(in: DataInput): Unit = {
    splitId = in.readInt
    numSplits = in.readInt
  }

  override def write(out: DataOutput): Unit = {
    out.writeInt(splitId)
    out.writeInt(numSplits)
  }

  override def toString = getClass.getName + ": task " + splitId + " of " + numSplits
}

