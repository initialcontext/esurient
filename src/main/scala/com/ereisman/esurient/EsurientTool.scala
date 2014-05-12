package com.ereisman.esurient


import org.apache.hadoop.util.{Tool,ToolRunner}
import org.apache.hadoop.conf.{Configuration,Configured}


object EsurientTool {

  def main(args: Array[String]): Unit = {
    System.exit(ToolRunner.run(new Configuration, new EsurientTool, args))
  }
}


class EsurientTool extends Configured with Tool {

  override def run(args: Array[String]): Int = {
    // TODO: parse the cmd line args (GenericOptionsParser + custom args)
    //       then add these to the Configuration and run the Job
    0 // return for now
  }
}
