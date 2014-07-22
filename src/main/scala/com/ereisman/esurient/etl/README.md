### Esurient ETL ###
This package is the implementation for an example Esurient job that pulls data down from any standard database. MySQL and Postgres support are included in the example package but any db that provides a JDBC driver can easily be added. Although individual ETL tasks are configured via the job's Properties file, some formatting classes can also be plugged in via the task runner class. See the `EsurientEtlTask` example class to see how an ETL job can be configured with plug-in classes to specify gziped TSV files as output.

### Running an example job ###
To set up and run the Esurient ETL job:

* Place a file on HDFS containing a JSON-serialized list of databases and connection info. Alternately, implement your own DatabaseConfigExtractor subclass (see `etl.format` package)

* Run `bin/setup-etl-job` supplying the args needed to customize the run (password, db name to look up in JSON config file, update or bootstrap table mode, etc.) - See `EsurientEtlMetadataManager` for examples of args you can pass.

* Setup script, if it was able to connect to the db you selected, will have deposited 2 files on hdfs: a JSON-based schema of the table to be snapshotted, and a Java Properties files configured for a db snapshot job on that table.

* Run `bin/esurient -j hdfs:///full/path/to/your/table-snapshot-job.properties` to execute the db snapshot job on the cluster. The default job will place one gzip file on HDFS for each Esurient process used in the job. The number will depend on the nature of the database (sharded, etc.)

Once you have bootstrapped the table, you can re-run the setup script to create a properties file appropriate for regular "update" runs over a time window. Running the update job on a regular basis will keep the accumulated table data fresh. Deduping the bootstrap and all accumulated update snapshots to obtain only the latest copy of each row is best performed in a MapReduce post-processing job.

