package com.ereisman.esurient.hadoop.io


import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class EsurientIntpuSplitSpec extends FlatSpec with ShouldMatchers {
  val DEFAULT = -1
  val ZERO = 0
  val ONE = 1
  val TWO = 2


  "An EsurientInputSplit" should "always expose the splitId and numSplits they are assigned at creation" in {
    val eis = new EsurientInputSplit(ONE, TWO)
    eis.splitId should be (1)
    eis.numSplits should be (2)
  }

  "An EsurientInputSplit" should "always expose the default splitId and numSplits when instantiated without arguments" in {
    val eis = new EsurientInputSplit
    eis.splitId should be (DEFAULT)
    eis.numSplits should be (DEFAULT)
  }

  "An EsurientInputSplit" should "return 0 from getLength" in {
    (new EsurientInputSplit).getLength should be (ZERO)
  }

  "An EsurientInputSplit" should "serialize and deserialize correctly" in {
    // serialize
    val eis = new EsurientInputSplit(TWO, ONE)
    val buffer = new java.io.ByteArrayOutputStream
    eis.write(new java.io.DataOutputStream(buffer))
    // deserialize
    val check = new java.io.DataInputStream(
      new java.io.ByteArrayInputStream(buffer.toByteArray)
    )
    val other = new EsurientInputSplit
    other.readFields(check)
    // assert
    other.splitId should be (2)
    other.numSplits should be (1)
  }
}
