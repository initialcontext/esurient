package com.ereisman.esurient.hadoop


import com.ereisman.esurient.EsurientConstants._

import com.ereisman.esurient.hadoop.io.{EsurientInputFormat,EsurientOutputFormat}
import com.ereisman.esurient.hadoop.mapreduce.EsurientMapper

import org.apache.hadoop.conf.{Configuration,Configured}
import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.{Tool,ToolRunner,GenericOptionsParser}

import scala.io.Source


object EsurientTool {

  def main(args: Array[String]): Unit = {
    System.exit( ToolRunner.run(new Configuration(true), new EsurientTool, args) )
  }
}


/**
 * com.ereisamn.esurient.EsurientTool is the driver class to be called by "hadoop jar" command
 */
class EsurientTool extends Configured with Tool {

  override def run(args: Array[String]): Int = {
    val conf = getConf
    conf.addResource("esurient-site.xml")
    injectJobPropsIntoConfiguration(args(0), conf)
    val job = Job.getInstance( conf, conf.get(ES_JOB_NAME, "Esurient Job") )
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
   * Job configuration in Esurient comes from a standard Java Properties file supplied in the
   * command line arguments to 'hadoop jar' or 'bin/esurient' launcher scripts. These properties
   * will be applied as overrides in the Hadoop Configuration key-value store, which jobs can
   * access at run time using <code>EsurientTask.Context.getConfiguration.get(key)</code>
   *
   * @param propsUri the path to the job's properties file (can be local or on HDFS)
   * @param conf the Hadoop Configuration we must populate with Esurient job-specific keys and values.
   * @return the Hadoop Configuration to pass to the Job we're submitting to the cluster.
   */
  def injectJobPropsIntoConfiguration(propsUri: String, conf: Configuration): Configuration = {
    propsUri match {
      case hdfsPropsUri: String if (hdfsPropsUri.startsWith("hdfs://")) => {
        val stream = Source.fromInputStream(FileSystem.get(conf).open(new Path(hdfsPropsUri)), "UTF-8")
        addSourceToConfiguration( conf, stream.mkString )
      }
      case localProps: String => {
        addSourceToConfiguration( conf, Source.fromFile(new java.io.File(localProps), "UTF-8").mkString )
      }
      case _ =>
        throw new RuntimeException("Local job properties file was not found at: " + propsUri)
    }
    conf
  }

  def addSourceToConfiguration(conf: Configuration, jobProps: String): Unit = {
    jobProps.split("""\n""").map { (line: String) =>
      val kv = line.split("""=""")
      conf.set(kv(0).trim, kv(1).trim)
    }
  }
}
