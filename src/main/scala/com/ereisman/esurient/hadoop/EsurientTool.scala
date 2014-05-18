package com.ereisman.esurient.hadoop


import com.ereisman.esurient.EsurientConstants._
import com.ereisman.esurient.hadoop.io.{EsurientInputFormat,EsurientOutputFormat}
import com.ereisman.esurient.hadoop.mapreduce.EsurientMapper
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.{Tool,ToolRunner,GenericOptionsParser}
import org.apache.hadoop.conf.{Configuration,Configured}


object EsurientTool {

  def main(args: Array[String]): Unit = {
    Configuration.addDefaultResource("esurient-site.xml")
    System.exit( ToolRunner.run(new Configuration(true), new EsurientTool, args) )
  }
}


/**
 * com.ereisamn.esurient.EsurientTool is the driver class to be called by "hadoop jar" command
 */
class EsurientTool extends Configured with Tool {

  override def run(args: Array[String]): Int = {
    setConf( injectJobPropsIntoConfiguration(args(0), getConf) )
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
   * Job configuration in Esurient comes from a standard Java Properties file found in the
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
      case hdfsPropsUri: String if (hdfsPropsUri.startsWith("hdfs://")) =>
        conf.addResource( new org.apache.hadoop.fs.Path(hdfsPropsUri) )
      case localProps: String =>
        conf.addResource( new java.io.FileInputStream(localProps) )
      case _ =>
        throw new RuntimeException("Local job properties file was not found at: " + propsUri)
    }
    conf
  }
}
