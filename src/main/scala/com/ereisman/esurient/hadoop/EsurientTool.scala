package com.ereisman.esurient.hadoop


import com.ereisman.esurient.EsurientConstants._
import com.ereisman.esurient.hadoop.io.{EsurientInputFormat,EsurientOutputFormat}
import com.ereisman.esurient.hadoop.mapreduce.EsurientMapper
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.{Tool,ToolRunner,GenericOptionsParser}
import org.apache.hadoop.conf.{Configuration,Configured}


object EsurientTool {

  def main(args: Array[String]): Unit = {
    // if HADOOP_CONF_DIR is on the classpath, this will pick up the site.xml files
    System.exit( ToolRunner.run(new Configuration(true), new EsurientTool, args) )
  }
}


/**
 * com.ereisamn.esurient.EsurientTool is the driver class to be called by "hadoop jar" command
 */
class EsurientTool extends Configured with Tool {

  override def run(args: Array[String]): Int = {
    setConf( parseArgsIntoConfiguration(args, getConf) )
    val job = Job.getInstance( getConf, getConf.get(ES_JOB_NAME, "Esurient Job") )
    job.setMapperClass(classOf[EsurientMapper])
    job.setInputFormatClass(classOf[EsurientInputFormat])
    job.setOutputFormatClass(classOf[EsurientOutputFormat])
    job.setJarByClass(this.getClass)
    job.setSpeculativeExecution(false)
    job.setNumReduceTasks(0)
    try {
      job.submit ; 0
    } catch {
      case e: Exception => throw e ; -1 // this is silly
    }
  }

  /**
   * Job configuration in Esurient comes from the command line OR optionally from standard K=V
   * Java property files.
   *      <p>
   *      Global job configurations (or values meant to override those in the local host machine's
   *      HADOOP_CONF_DIR) can be placed in HADOOP_CONF_DIR named "esurient-site.xml"
   *      <p>
   *      Job specific Configuration values can come as command line arguments to "hadoop jar"
   *      in the form --esurient.arg.X.Y.Z=VALUE where X, Y, Z are any number of dot-separated words.
   *      Each configuration key entered in this way will be injected into the Hadoop Configuration
   *      for your cluster job, and will be exposed for the user via the EsurientTask.Context object.
   *      <p>
   *      There are a number of framework-specific command line arguments of this form you can view
   *      by entering --help as a command line argument. These include configuration keys to allowing
   *      the user to select the number of Mapper tasks to be run and the name of a user-defined class
   *      Inheriting from EsurientTask to instantiate in each host Mapper.
   */
  def parseArgsIntoConfiguration(args: Array[String], conf: Configuration): Configuration = {
    // TODO: make useful Configuration values out of args, return conf
    conf
  }
}
