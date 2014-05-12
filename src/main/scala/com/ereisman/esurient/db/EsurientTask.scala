package com.ereisman.esurient.db


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.util.Progressable

import org.apache.log4j.Logger

import com.ereisman.esurient.hadoop.io.EsurientInputSplit


object EsurientTask {
  val LOG = Logger.getLogger(classOf[EsurientTask])
  // TODO: make some regexes that will help with the parsing
}


// TODO: call prog#progress() a lot when looping or in sep. thread during queries?
//class EsurientTask(val taskId: Int, val totalTasks: Int, val conf: Configuration, val prog: Progressable) {
class EsurientTask(context: Mapper[NullWritable, NullWritable, NullWritable, NullWritable]#Context) {
  val taskId = context.getInputSplit.asInstanceOf[EsurientInputSplit].splitId
  val totalTasks = context.getInputSplit.asInstanceOf[EsurientInputSplit].numSplits
  val conf = context.getConfiguration
  val taskIdConfKey = "esurient.task." + taskId
  val task = conf.get(taskIdConfKey, null) match {
    case line: String => parseTaskToMap(line)
    case _            => throw new RuntimeException("Configuration key '" + taskIdConfKey + "' not found, aborting.")
  }
  // TODO: set up DB metadata objects to track connections etc. before we run the task
  runTask

  def runTask: Unit = { /* TODO */ }

  // SUBJECT TO CHANGE - Could store as JSON or Base64 encode as in Cascading Configuration metadata fields
  def parseTaskToMap(line: String): Map[String, String] = {
    line.split("""\s*;\s*""").foldLeft(Map[String, String]()) { (acc, token) =>
      acc ++ (token.split("""=""") match {
        case kv: Array[String] if (kv.size == 2) => Map(kv(0) -> kv(1))
        case _               => throw new RuntimeException("Task Configuration found bad token, aborting.") 
      })
    }
  }
}

