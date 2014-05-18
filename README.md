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
Clone or download the Esurient repo to a host machine with a functioning Hadoop client installed, including
`HADOOP_HOME`, `JAVA_HOME` env vars, and where `HADOOP_CONF_DIR` is set and populated with the proper Hadoop
`*-site.xml` files.

The user must configure each individual job with a local (or HDFS hosted) Java properties file containing keys and values to be injected into the Hadoop Configuration at runtime. It is left to the user to avoid accidentally overriding core Hadoop or Esurient framework Configuration values...unless this is what is intended by the user :p

To avoid repeating boilerplate in job properties files, a user may provide global Configuration property 
overrides to be applied to every Esurient job by placing an `esurient-site.xml` file in `HADOOP_CONF_DIR`.
This file must be XML formatted in name and value pairs just like the Hadoop system `*-site.xml` files.

Execute `bin/build` to build the project. You may place user-defined code into the examples package for easy "fat jar"
job deployments, or compile your user code against Esurient's build products if they will be packaged separately.

User defined classes must subclass `com.ereisman.esurient.EsurientTask`, and define one method, `execute` which accepts a single parameter: the `EsurientTask.Context` object. The Context wraps all Hadoop/Esurient framework metadata and user-defined job data, including the spawned task's _unique runtime task ID_. See code in the `com.ereisman.esurient.examples` package.

Execute `bin/esurient -p path/to/job.properties -j path/to/esurient.jar`, or alternately run `hadoop jar` parameterized as shown in `bin/esurient`

That's it! You can pretty much do anything you want in your job code, you are no longer working within the MapReduce paradigm.

### DISCLAIMER ###
Use additional caution when you share a cluster with other users, especially those running MapReduce jobs. Esurient specializes in giving you all the rope you need to hang yourself, so _use common sense_.

