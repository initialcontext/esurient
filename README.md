esurient
========

### Purpose ###
A Lightweight framework for safely running generic distributed computing processes on a Hadoop cluster.
Written in Scala, the user can define a driver class extending `com.ereisman.esurient.EsurientTask` and define a single
`execute` method taking a `EsurientTask.Context` object as its argument.

There are no keys and values to deal with, no mandatory sort, shuffle, or reduce stages. Task's progress heartbeats can be handled manually by the user's task or automatically without danger of zombie processes holding cluster slots. Your tasks get to piggyback on Hadoop's own fault-tolerance mechanisms: failed tasks will be rerun on an individual level several times before the whole job is ruled a fail by Hadoop.

The `EsurientTask.Context` object provides runtime access to low-level Hadoop plumbing API's _and_ the job's Hadoop Configuration which is injected with user-defined job properties by Esurient at runtime. A _unique task ID_ is also injected so the user can deterministically assign work to each or any task in the job. Since Esurient assigns monotonically increasing integer task ID's, partitioning tasks into work groups is easy.

See the code in the `examples` package for more ideas. Esurient is ideal for users who need to do distributed computing that isn't a natural fit for MapReduce. Some examples:

* Node-local or HDFS filesystem chores in a distribtued way
* Database snapshots or other ETL (See Esurient example jobs and 'etl' package)
* Message passing or RPC on a Hadoop cluster? Recieve/generate event streams
* Host a long-lived service on a set number of your Hadoop slots?
* Communicate with external services
* Do something better suited to a YARN cluster on the cluster you have now


### Limitations ###
This technique isn't magic - although we get a user-defined number of Mapper processes, unique task IDs, and global job configuration to share marching orders, we don't have control over which cluster nodes will host each Mapper process. This makes some activities a bit trickier. To bootstrap message passing, cluster processes might communicate with the process that launched the job. For coordinating more complex activities (leader election, etc.) tools like Apache Zookeeper come in handy. Other projects using the same technique to execute on Hadoop include Apache Giraph and Apache Sqoop, among others.


### Building the Project ###
Ensure you have Java, a Hadop client appropriate to your cluster version, and Maven 3 installed and properly configured on your build machine. You won't need to have Scala installed on the cluster or your build machine.

Edit the POM.xml file in the Esurient distribution's root directory. Set the configuration property __hadoop.version__ to match your own Hadoop distribution, or Esurient is not likely to run on your cluster. Adjust any other dependencies or configuration variables to taste, especially if user code will be bundled into the Esurient build rather than including the Esurient jar in a user project.  

Execute `bin/build` to build the project, execute the test suites, and produce the build artifacts. The build produces the typical set of Maven build artifacts located in the `target` directory, including:
* a JAR file (not including library dependencies declared in the POM.xml) to be run on a cluster that has those deps installed and on the classpath already.
* a "far jar" JAR file (including all library deps) to be run with `hadoop jar` as a standalone application.
* a "bundle" tar.gz which is a full "binary distribution" including bin scripts, the fat jar.

### User Defined Jobs ###
User defined classes must subclass `com.ereisman.esurient.EsurientTask`, and define one method, `execute` which accepts a single parameter: the `EsurientTask.Context` object. The Context wraps all Hadoop/Esurient framework metadata and user-defined job data, including the spawned task's _unique runtime task ID_. See code in the `com.ereisman.esurient.examples` package.

You may place user-defined code into the examples package inside the Esurient project, and it will be built and packaged inside the Esurient JAR artifacts.

For more complex user projects, add the Esurient fat jar as a dependency. If the user opts to depend on Esurient in this way, one additional step must be taken at runtime. When calling `hadoop jar` or `bin/esurient` to execute the job, the user must supply the Hadoop -libjars arg and a local path to the user's jar. In this case, note that the `hadoop jar` call must still actually execute against the Esurient fat jar and the EsurientTool driver class as seen in the `bin/esurient` script. User code packaged outside the Esurient jar is simply a library of an Esurient run. Similarly, libraries required by user code should also be included via -libjars. See the Hadoop documentation for details.


### Running Jobs ###
Before executing an Esurient job, the user must populate a Java properties file with all the user-defined keys and values to be injected into the Hadoop Configuration at runtime. This could include keys known only within your application code, keys that alter the Esurient framework's behavior, or anything you would pass to an ordinary Hadoop job. It is left to the user to avoid accidentally overriding core Hadoop or Esurient framework Configuration values unless this is the user's explicit intention :p

Examine the `esurient-example-job.properties` file as a basis for your own job configs. To execute user-defined task code, the properties file must include the fully-qualified classname of the user's EsurientTask implementation. Most users will also want to specify the number of cluster tasks to launch as well.

To avoid repeating boilerplate in a set of similar job properties files, a user may provide global Configuration property overrides to be applied to every Esurient job by placing an `esurient-site.xml` file in `HADOOP_CONF_DIR`. This file must be XML formatted in name and value pairs just like the Hadoop system `*-site.xml` files.

Execute `bin/esurient -p path/to/job.properties -j path/to/esurient.jar`, or alternately run `hadoop jar` parameterized as shown in `bin/esurient` addition arguments to `bin/esurient` are routed to it's internal `hadoop jar` call.

That's it! You can pretty much do anything you want in your job code, you are no longer working within the MapReduce paradigm.


### ETL With Esurient ###
The Esurient `etl` package includes a fully functional, customizable, database snapshot job that has been tested on production workloads. See the README.md in the `src/main/scala/com/ereisman/esurient/etl` directory for instructions.


### Warning ###
This software uses a stable and proven method for running non-MapReduce, long-lived processes on a Hadoop cluster. However, Esurient specializes in giving you all the rope you need to hang yourself, so _use common sense_. Additional caution is advised when your Esurient job will share a cluster with other users' jobs. Make sure job authors understand the Hadoop Configuration values they override, and the resource commitments job tasks will require on the cluster.
