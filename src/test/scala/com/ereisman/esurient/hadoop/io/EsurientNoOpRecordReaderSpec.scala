package com.ereisman.esurient.hadoop.io


import com.ereisman.esurient.EsurientConstants._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.NullWritable

import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class EsurientNoOpRecordReaderSpec extends FlatSpec with ShouldMatchers {
  val ZERO = 0.0f
  val ONE = 1.0f


  "An EsurientNoOpRecordReader" should "report progress correctly" in {
    val enorr = new EsurientNoOpRecordReader
    enorr.getProgress should be (ZERO)
    enorr.nextKeyValue
    enorr.getProgress should be (ONE)
  }

  "An EsurientNoOpRecordReader" should "produce a NullWritable key" in {
    val enorr = new EsurientNoOpRecordReader
    enorr.getCurrentKey should be (NullWritable.get)
  }

  "An EsurientNoOpRecordReader" should "produce a NullWritable value" in {
    val enorr = new EsurientNoOpRecordReader
    enorr.getCurrentValue should be (NullWritable.get)
  }

  "An EsurientNoOpRecordReader" should "return only one key-value pair" in {
    val enorr = new EsurientNoOpRecordReader
    enorr.nextKeyValue should be (true)
    enorr.nextKeyValue should be (false)
    enorr.nextKeyValue should be (false)
  }
}
