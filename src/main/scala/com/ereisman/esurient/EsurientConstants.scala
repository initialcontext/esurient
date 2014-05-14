package com.ereisman.esurient


object EsurientConstants {
    
    // Hadoop Configuration keys ///////////////////////////////////
    val ES_DB_TYPE                            = "esurient.db.type"
    // is this task snapping 
    val ES_SHARDED_DB                         = "esurient.sharded.db"
    // to be combined with taskId
    val ES_TASK                               = "esurient.task."
    val ES_NUM_TASKS                          = "esurient.num.tasks"
    val ES_OUTPUT_PATH                        = "esurient.output.path"
    val ES_TASK_CLASS_NAME                    = "esurient.task.class.name"
    val ES_TASK_AUTO_HEARTBEAT                = "esurient.task.auto.heartbeat"
    val ES_THIS_TASK_ID                       = "esurient.this.task.id"
    val ES_TASK_COUNT                         = "esurient.task.count"
    val ES_TASK_AUTO_HEARTBEAT_MILLIS         = "esurient.task.auto.heartbeat.millis"

    // Hadoop Configuration Default Values /////////////////////////
    val ES_NUM_TASKS_DEFAULT                  = 20
    val ES_TASK_AUTO_HEARTBEAT_MILLIS_DEFAULT = 8000

    // Misc constants //////////////////////////////////////////////
    val DEFAULT_OUTPUT_DIR                    = "esurient_job_output_dir_" + (new java.util.Date).getTime
    val ERROR_NO_DB_TYPE_SET                  = "ERROR_NO_DB_TYPE_SET"
}
