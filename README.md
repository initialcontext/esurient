esurient
========

### Purpose ###
A Lightweight framework for running generic distributed computing processes on a Hadoop cluster.
Written in Scala, the user can define a driver class extending `com.ereisman.esurient.EsurientTask` and define a single
`execute` method defining the work the task will peform.

Esurient is just a utility for times where such a thing is useful, nothing more. It's been run at scale on production workloads in a scripting context (see `examples` and `etl` packages), but it's not fancy, and not a replacement for upgrading to YARN.

### Limitations ###
Esurient jobs don't have control over which cluster nodes will host each process. To bootstrap message passing, cluster processes might communicate with the process that launched the job. For coordinating more complex activities (leader election, etc.) tools like Apache Zookeeper come in handy.

### Building the Project ###
Ensure you have Java and Maven 3 installed on your build machine. You won't need Hadoop or Scala installed.

Edit the POM.xml file in the Esurient distribution's root directory. Set the configuration property __hadoop.version__ to match your own Hadoop distribution. Adjust any other dependencies or configuration variables to taste.  

Executing `bin/build` produces the typical set of Maven build artifacts located in the `target` directory. This includes a tarball, a JAR, and a "fat" JAR (includes runtime dependencies.)


### User Defined Jobs ###
User defined classes must subclass `com.ereisman.esurient.EsurientTask`, and define one method, `execute` which accepts a single parameter: the `EsurientTask.Context` object. The Context wraps all Hadoop/Esurient framework metadata and user-defined job data, including the spawned task's _unique runtime task ID_. See code in the `com.ereisman.esurient.examples` package.

User projects can depend on the Esurient fat JAR at compile and runtime to launch jobs from any machine with an appropriate Hadoop client installed.

### Running Jobs ###
Before executing an Esurient job, place all user-defined keys and values into a Java Properties file. These will be injected into the Hadoop Configuration at runtime by the framework. Examine the `esurient-example-job.properties` file as a basis for your own job configs. To execute user-defined tasks, the properties file must include the fully-qualified classname of the user's task implementation.

Execute `bin/esurient -p path/to/job.properties -j path/to/esurient.jar`, or alternately run `hadoop jar` parameterized as shown in `bin/esurient` additional arguments to `bin/esurient` are routed to it's internal `hadoop jar` call.


### ETL With Esurient ###
The Esurient `etl` package includes a customizable, parallel database snapshot job for MySQL or Postgress sharded (or non-sharded) databases. See the README.md in the `src/main/scala/com/ereisman/esurient/etl` directory for instructions.


### Warning ###
Make sure job authors understand the Hadoop Configuration values they override, and the resource commitments job tasks will require on the cluster. An Esurient job's properties file can override any Hadoop configuration value that is not marked "final" in the cluster `*-site.xml` files.
 
