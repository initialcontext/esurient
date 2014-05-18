package com.ereisman.esurient

/**
 * Constant definitions for Esurient project and its example jobs.
 * Includes Hadoop Configuration keys and their default values, and
 * misc. framework values.
 */
object EsurientConstants {

    // Hadoop Configuration Keys ///////////////////////////////////
    val ES_TASK_CLASS_NAME                    = "esurient.task.class.name"
    val ES_TASK_AUTO_HEARTBEAT                = "esurient.task.auto.heartbeat"
    val ES_THIS_TASK_ID                       = "esurient.this.task.id"
    val ES_TASK_GROUPS                        = "esurient.task.groups"
    val ES_TASK_COUNT                         = "esurient.task.count"
    val ES_TASK_AUTO_HEARTBEAT_MILLIS         = "esurient.task.auto.heartbeat.millis"
    val ES_LOG_HEARTBEATS                     = "esurient.log.heartbeats"
    // to be combined with taskId to form the single-task-specific config keys
    val ES_TASK                               = "esurient.task."
    val ES_JOB_NAME                           = "esurient.job.name"
    // Users can place global config overrides in HADOOP_CONF_DIR as "esurient-site.xml"
    // this config key can be set to the path of a 2nd, job-specific override file.
    val ES_SITE_FILE                          = "esurient.site.file"

    // Task-Specific Configuration Keys (for user tasks, these will be defined in the application code)
    val ES_DB_TYPE                            = "esurient.db.type"
    val ES_SHARDED_DB                         = "esurient.sharded.db"
 
    // Hadoop Configuration Default Values /////////////////////////
    val ES_TASK_COUNT_DEFAULT                  = 20
    val ES_TASK_AUTO_HEARTBEAT_MILLIS_DEFAULT = 8000

    // Misc Constants //////////////////////////////////////////////
    val DEFAULT_OUTPUT_DIR                    = "esurient_job_output_dir_" + (new java.util.Date).getTime
    val ERROR_NO_DB_TYPE_SET                  = "ERROR_NO_DB_TYPE_SET"
}
