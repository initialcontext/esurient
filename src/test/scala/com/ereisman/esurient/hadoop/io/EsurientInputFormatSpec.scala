package com.ereisman.esurient.hadoop.io


import com.ereisman.esurient.EsurientConstants._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class EsurientInputFormatSpec extends FlatSpec with ShouldMatchers {
  val TWO = 2


  "An EsurientInputFormat" should "return the number of InputSplits requested in the Configuration" in {
    val conf = new Configuration(false)
    conf.setInt(ES_TASK_COUNT, TWO)
    val splitList = (new EsurientInputFormat).getSplits(new Job(conf))
    splitList.size should be (2)
  }
}
