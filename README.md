esurient
========

## Purpose ##
A Lightweight framework for safely running generic distributed computing processes on a Hadoop cluster.
Written in Scala, the user can define a driver class extending `com.ereisman.esurient.EsurientTask` and define a single
`execute` method taking a `EsurientTask.Context` object as its argument.

There are no keys and values to deal with, no mandatory sort, shuffle, or reduce stages. Task heartbeats are handled for you
by default so you can focus on _getting stuff done_. Your tasks get to piggyback on Hadoop's own fault-tolerance mechanisms.

The EsurientTask.Context object provides runtime access to low-level Hadoop plumbing API's _and_ the job's Hadoop Configuration which is injected with user-defined job properties by Esurient at runtime. A _unique task ID_ is also injected so the user can deterministically assign work to each or any task in the job. Since Esurient assigns monotonically increasing integer task ID's, partitioning tasks into work groups is easy.

See the code in the `examples` package for more ideas. In conclusion, Esurient is ideal for users who:

* Don't know anything about Hadoop but have work to do and an idle Hadoop cluster?

* Want to do node-local or HDFS filesystem chores in a distribtued way?

* Message passing or RPC on a Hadoop cluster? Recieve/generate event streams?

* Host another long-lived service on a set number of your Hadoop slots?

* Communicate with databases or other external servies, spawn actors/threads in each of your tasks, write your own app, but don't have a YARN cluster yet?

* Need to do something that MapReduce just isn't good at? Don't want to be told how to do it?


## Instructions ##
Ensure you have Java, a Hadop client appropriate to your cluster version, all associated configuration files and env vars set, and Maven 3 installed. You won't need to have Scala installed on the cluster or your build machine.

Clone or download the Esurient repo to a host machine with a functioning Hadoop client installed, including `HADOOP_HOME`, `JAVA_HOME` env vars, and where `HADOOP_CONF_DIR` is set and populated with the proper Hadoop `*-site.xml` files.

Edit the POM.xml file in the top-level distribution directory. Set the configuration property __hadoop.version__ to match your own Hadoop distribution, or Esurient is not likely to run on your cluster. Adjust any other dependencies or configuration variables to taste, especially if user code will be bundled into the Esurient build rather than including the Esurient jar in a user project.  

Execute `bin/build` to build the project. The build produces the typical set of Maven build artifacts located in the `target` directory, including:
* a JAR file (not including library dependencies declared in the POM.xml) to be run on a cluster that has those deps installed and on the classpath already.
* a "far jar" JAR file (including all library deps) to be run with `hadoop jar` as a standalone application.
* a "bundle" tar.gz which is a full "binary distribution" including bin scripts, the fat jar.

User defined classes must subclass `com.ereisman.esurient.EsurientTask`, and define one method, `execute` which accepts a single parameter: the `EsurientTask.Context` object. The Context wraps all Hadoop/Esurient framework metadata and user-defined job data, including the spawned task's _unique runtime task ID_. See code in the `com.ereisman.esurient.examples` package.

You may place user-defined code into the examples package inside the Esurient project, and it will be built and packaged inside the Esurient JAR artifacts.

For more complex projects, it is recommended to simply add the Esurient fat jar as a dependency. If the user opts to depend on Esurient in this way, one additional step must be taken at runtime. When calling `hadoop jar` or `bin/esurient` to execute the job, the user must supply the Hadoop -libjars arg and a local path to the user's jar. In this case, note that the `hadoop jar` call must still actually execute against the Esurient fat jar and the EsurientTool driver class as seen in the `bin/esurient` script. User code packaged outside the Esurient jar is simply a library of an Esurient run. Similarly, libraries required by user code should also be included via -libjars. See the Hadoop documentation for details.

Before executing an Esurient job, the user must populate a Java properties file with all the user-defined keys and values to be injected into the Hadoop Configuration at runtime. This could include keys known only within your application code, keys that alter the Esurient framework's behavior, or anything you would pass to an ordinary Hadoop job. It is left to the user to avoid accidentally overriding core Hadoop or Esurient framework Configuration values unless this is the user's explicit intention :p

Examine the `esurient-example-job.properties` file as a basis for your own job configs. To execute user-defined task code, the properties file must include the fully-qualified classname of the user's EsurientTask implementation. Most users will also want to specify the number of cluster tasks to launch as well.

To avoid repeating boilerplate in a set of similar job properties files, a user may provide global Configuration property overrides to be applied to every Esurient job by placing an `esurient-site.xml` file in `HADOOP_CONF_DIR`. This file must be XML formatted in name and value pairs just like the Hadoop system `*-site.xml` files.

Execute `bin/esurient -p path/to/job.properties -j path/to/esurient.jar`, or alternately run `hadoop jar` parameterized as shown in `bin/esurient` addition arguments to `bin/esurient` are routed to it's internal `hadoop jar` call.

That's it! You can pretty much do anything you want in your job code, you are no longer working within the MapReduce paradigm.

### Warning ###
This software is a stable and proven method for running non-MapReduce, long-lived processes on a Hadoop cluster. However, Esurient specializes in giving you all the rope you need to hang yourself, so _use common sense_. Additional caution is advised when your Esurient job will share a cluster with other users' jobs.
