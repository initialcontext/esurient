package com.ereisman.esurient.hadoop


import org.apache.hadoop.util.{Tool,ToolRunner,GenericOptionsParser}
import org.apache.hadoop.conf.{Configuration,Configured}


object EsurientTool {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration(true)
    val nonHadoopArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    System.exit(
      ToolRunner.run(conf, new EsurientTool, nonHadoopArgs)
    )
  }
}


class EsurientTool extends Configured with Tool {

  override def run(args: Array[String]): Int = {
    // TODO: parse args that were not delivered to "hadoop jar" commmand
    //       by the GenericOptionsParser, add those values to the Configuration
    //       and kick off the job run!
    0
  }
}
