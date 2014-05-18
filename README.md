esurient
========

A Lightweight framework for safely running generic distributed computing processes on a Hadoop cluster.
Written in Scala, the user can define a driver class extending com.ereisman.esurient.EsurientTask and define a single
execute() method taking a EsurientTask.Context object as its argument. You'll be up and runnning in 5 minutes!

There are no pesky keys and values to deal with, no mandatory Reduce or Shuffle/Sort stage. Health heartbeating is handled for you
by default so you can focus on _getting stuff done_. Your tasks get to piggyback on Hadoop's own fault-tolerance mechanisms.

The EsurientTask.Context object provides low-level access to all Hadoop-level plumbing and the job's Hadoop Configuration
injected with user-defined job properties _and a unique task ID_ so the user can deterministically assign work to each or any
task in the job. See the example jobs for more ideas. Since Esurient assigns monotonically increasing integer task ID's, partitioned task groups of any sort can also be composed with ease.

Don't know anything about Hadoop but have work to do and an idle Hadoop cluster?

Want to do local or HDFS filesystem chores in a distribtued way?

Message passing or RPC on a Hadoop cluster? Recieve/generate event streams?

Host another long-lived service on a set number of your Hadoop slots?

Database access, spawn actors/threads, whatever?

Don't have a YARN (MRv2) Hadoop cluster yet?

Need to do something that MapReduce just isn't good at? Don't want to be told how to do it?

# Instructions #
Clone or download the Esurient repo to a host machine with a functioning Hadoop client installed, including
`HADOOP_HOME`, `JAVA_HOME` env vars, and where `HADOOP_CONF_DIR` is set and populated with the proper Hadoop
`*-site.xml` files. The user may provide global job overrides to be applied to the configuration of every job
run by placing an `esurient-site.xml` file (formated identically to the Hadoop site files) in `HADOOP_CONF_DIR`

In addition, the user should configure each individual job with a local (or HDFS hosted) Java properties file
containing keys and values to be injected into the Hadoop Configuration each task will recieve at runtime. These
keys and values must be chosen with care to avoid accidentally overriding Hadoop or Esurient framework data...unless
this is what is intended by the user. In which case...have fun!

Execute `bin/build` to build the project. You may place user-defined code into the examples package for easy "fat jar"
job deployments, or compile your user code against Esurient's build products if they will be packaged separately.

User defined classes must subclass EsurientTask, and define one method, `execute()` which accepts a single parameter:
the `EsurientTask.Context` object which wraps all Hadoop/Esurient framework metadata and user-defined job data, including
the spawned task's unique runtime task ID. The jobs in the `com.ereisman.esurient.examples` package will provide insight.

Execute `bin/esurient -p path/to/job.properties -j path/to/esurient.jar`, or alternately run `hadoop jar` parameterized
as shown in `bin/esurient`

That's it! You can pretty much do anything you want in your job code, you are no longer working within the MapReduce paradigm.
Take care however; if you're submitting jobs to a shared cluster, you will need to make sure you code is a "good citizen" of that
cluster. Consult with your Hadoop operations team before performing various forms of "high load" tasks on a production cluster.

