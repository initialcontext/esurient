package com.ereisman.esurient


/**
 * Constant definitions for Esurient project and its example jobs.
 * Includes Hadoop Configuration keys and their default values, and
 * misc. framework values.
 */
object EsurientConstants {

    // Esurient/Hadoop Configuration Keys //////////////////////////////
    val ES_TASK_CLASS_NAME                    = "esurient.task.class.name"
    val ES_JOB_NAME                           = "esurient.job.name"
    val ES_THIS_TASK_ID                       = "esurient.this.task.id"
    val ES_TASK_GROUPS                        = "esurient.task.groups"
    val ES_TASK_COUNT                         = "esurient.task.count"
    val ES_TASK_AUTO_HEARTBEAT                = "esurient.task.auto.heartbeat"
    val ES_TASK_AUTO_HEARTBEAT_MILLIS         = "esurient.task.auto.heartbeat.millis"
    val ES_LOG_HEARTBEATS                     = "esurient.log.heartbeats"
    val ES_JOB_TIMESTAMP                      = "esurient.job.timestamp" // global start stamp for an esurient job
    val ES_METRICS_HOST_PORT                  = "esurient.metrics.host.port" // host:port string for pinging monitoring endpoint
    val ES_HEARTBEAT_METRICS_MSG              = "esurient.heartbeat.metrics.msg" // see EsurientStats
    val ES_METRICS_KEY                        = "esurient.metrics.key" // subpartition metrics reported by job name or similar

    // Hadoop Configuration Default Values /////////////////////////
    val ES_ERROR_CODE                         = -1
    val ES_TASK_COUNT_DEFAULT                 = 20
    val BUFFER_SIZE                           = 65536 
    val ES_TASK_AUTO_HEARTBEAT_MILLIS_DEFAULT = 8000
    val ES_HEARTBEAT_METRICS_MSG_DEFAULT      = "esurient.heap.mb.%s.task%s %s" // %s is: job name, taskId, integral metric value
    val ES_METRICS_SEND_TIMEOUT               = 1000 // timeout fast if metrics services are sluggish

    // to be combined with taskId to form the single-task-specific config keys
    // in practice users can make up their own or use this as a prefix, etc.
    val ES_TASK                               = "esurient.task."

    // Database ETL connection config keys ///////////////////////////
    val ES_DB_SHARDED_TABLE                   = "esurient.db.sharded.table" // boolean
    val ES_DB_HOSTNAME                        = "esurient.db.hostname"
    val ES_DB_PORT                            = "esurient.db.port"
    val ES_DB_DBNAME                          = "esurient.db.dbname" // name used in query conf
    val ES_DB_TABLE_NAME                      = "esurient.db.table.name"
    val ES_DB_MODE                            = "esurient.db.mode" // bootstrap or update table?
    val ES_DB_USERNAME                        = "esurient.db.username"
    val ES_DB_PASSWORD                        = "esurient.db.password"
    val ES_DB_DATABASE                        = "esurient.db.database" // which entries from --dbConfig file to use in job props
    val ES_DB_TYPE                            = "esurient.db.database.type" // MySQL, Postgres, etc.
    val ES_DB_DATABASE_CONFIG_EXTRACTOR_TYPE  = "esurient.db.config.extractor.type"

    // Database ETL task config keys ////////////////////////////////
    val ES_DB_RETRIES                         = "esurient.db.retries"
    val ES_DB_CONFIG_FILE_PATH                = "esurient.db.config.file.path" // --dbConfig path to db metadata file on HDFS
    val ES_DB_OUTPUT_FORMAT                   = "esurient.db.output.format"
    val ES_DB_BASE_OUTPUT_PATH                = "esurient.db.base.output.path"
    val ES_DB_UPDATE_COLUMN                   = "esurient.db.update.column" // to use in update mode & schema gen
    val ES_DB_DEDUP_COLUMN                   = "esurient.db.dedup.column"   // to use in update mode & schema gen
    val ES_DB_UPDATE_WINDOW_SECS              = "esurient.db.update.window.secs" // also for update mode
    val ES_DB_OUTPUT_COMP_TYPE                = "esurient.db.output.compression.type"

    // Database ETL default constants ///////////////////////////////
    val ES_DB_TYPE_MYSQL                      = "mysql"
    val ES_DB_TYPE_POSTGRES                   = "postgres"
    val ES_DB_CONNECTION_TIMEOUT_DEFAULT      = 10000 // milliseconds
    val ES_DB_RETRIES_DEFAULT                 = 3
    val ES_DB_QUICK_PAUSE_MILLIS              = 1000
    val ES_DB_RETRY_SLEEP_MILLIS              = 5000
    val ES_DB_UPDATE_WINDOW_SECS_DEFAULT      = (86400 * 3) // 3 days
    val ES_DB_BOOTSTRAP_MODE                  = "bootstrap"
    val ES_DB_UPDATE_MODE                     = "update"
    val ES_DB_UPDATE_COLUMN_DEFAULT           = "update_date"
    val ES_DB_SCHEMA_FILE_SUFFIX              = "-schema.json"
    val ES_DB_JOB_PROPS_FILE_SUFFIX           = "-snapshot-job.properties"
    val ES_DB_BASE_OUTPUT_PATH_DEFAULT        = "/user/" + System.getProperty("USER") + "/etl/snapshots"
    val ES_DB_HEARTBEAT_METRICS_MSG_DEFAULT   = "hadoop.etl.snapshot.heap.mb.%s.task%s %s"
    val ES_DB_OUTPUT_FORMAT_TSV               = "tsv"
    val ES_DB_OUTPUT_COMP_TYPE_DEFAULT        = "org.apache.hadoop.io.compress.GzipCodec"
}

