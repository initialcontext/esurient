esurient
========

A Lightweight framework for safely running generic distributed computing processes on a Hadoop MapReduce Version 1 cluster.
Written in Scala, the user can define a driver class extending com.ereisman.esurient.EsurientTask and define a single
execute() method taking a EsurientTask.Context object as its argument.

There are no pesky keys and values to deal with, no mandatory reduce or shuffle/sort stage. Health heartbeating is handled
for you. The Context object provides low-level access to all the Hadoop underpinnings of the job and the Hadoop Configuration
full of task metadata, command line args, _and_ a unique task ID so the user can deterministically assign work to each or any
task in the job. Since Esurient assigns monotonically increasing integer task ID's, partitioned task groups of any sort can be
achieved just as easily. See the example jobs for more ideas.

Want to do local or HDFS filesystem chores in a distribtued way?
Message passing or RPC on a Hadoop cluster?
Host another long-lived service on a set number of your Hadoop slots?
Database access, spawn actors/threads, DDoS, whatever?
Don't know anything about Hadoop but have work to do and an idle Hadoop cluster?
Don't have a YARN (MRv2) Hadoop cluster yet?
Need to do something that MapReduce just isn't good at? Don't want to be told how to do it?

