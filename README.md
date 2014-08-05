esurient
========

### Purpose ###
A Lightweight framework for safely running generic distributed computing processes on a Hadoop cluster.
Written in Scala, the user can define a driver class extending `com.ereisman.esurient.EsurientTask` and define a single
`execute` method.

There are no keys and values, no mandatory sort, shuffle, or reduce stages. Task's progress heartbeats can be handled manually by the user's task or automatically. Your tasks piggyback on Hadoop's own fault-tolerance mechanisms. Each task can be assigned work in the job's properties file via unique task IDs.

See the code in the `examples` package for more ideas. Esurient is ideal for users who need to do distributed computing that isn't a natural fit for MapReduce. Some examples:

* Node-local or HDFS filesystem chores in a distribtued way
* Database snapshots or other ETL (See Esurient example jobs and `etl` package)
* Message passing or RPC on a Hadoop cluster
* Recieve event streams
* Host a long-lived service on a set number of your Hadoop slots
* Communicate with external services
* Do something better suited to a YARN cluster on the cluster you have now


### Limitations ###
Esurient jobs don't have control over which cluster nodes will host each Mapper process. This makes some activities tricky. To bootstrap message passing, cluster processes might communicate with the process that launched the job. For coordinating more complex activities (leader election, etc.) tools like Apache Zookeeper come in handy.

### Building the Project ###
Ensure you have Java, a Hadop client appropriate to your cluster version, and Maven 3 installed on your build machine. You won't need to have Scala installed on the cluster or your build machine.

Edit the POM.xml file in the Esurient distribution's root directory. Set the configuration property __hadoop.version__ to match your own Hadoop distribution. Adjust any other dependencies or configuration variables to taste.  

Execute `bin/build` to build the project, execute the test suites, and produce the build artifacts. The build produces the typical set of Maven build artifacts located in the `target` directory, including:
* a JAR file (not including library dependencies declared in the POM.xml) to be run on a cluster that has those deps installed and on the classpath already.
* a "far jar" JAR file (including all library deps) to be run with `hadoop jar` as a standalone application.
* a "bundle" tar.gz which is a full "binary distribution" including bin scripts, the fat jar.


### User Defined Jobs ###
User defined classes must subclass `com.ereisman.esurient.EsurientTask`, and define one method, `execute` which accepts a single parameter: the `EsurientTask.Context` object. The Context wraps all Hadoop/Esurient framework metadata and user-defined job data, including the spawned task's _unique runtime task ID_. See code in the `com.ereisman.esurient.examples` package.

You may place user-defined code into the examples package inside the Esurient project, and it will be built and packaged inside the Esurient JAR artifacts.


### Running Jobs ###
Before executing an Esurient job, place all user-defined keys and values into a Java Properties file. These will be injected into the Hadoop Configuration at runtime by the framework. Examine the `esurient-example-job.properties` file as a basis for your own job configs. To execute user-defined task code, the properties file must include the fully-qualified classname of the user's EsurientTask implementation.

Execute `bin/esurient -p path/to/job.properties -j path/to/esurient.jar`, or alternately run `hadoop jar` parameterized as shown in `bin/esurient` addition arguments to `bin/esurient` are routed to it's internal `hadoop jar` call.


### ETL With Esurient ###
The Esurient `etl` package includes a fully functional, customizable, database snapshot job that has been tested on production workloads. See the README.md in the `src/main/scala/com/ereisman/esurient/etl` directory for instructions.


### Warning ###
Make sure job authors understand the Hadoop Configuration values they override, and the resource commitments job tasks will require on the cluster. An Esurient job's properties file can override any Hadoop configuration value that is not marked "final" in the cluster `*-site.xml` files.

