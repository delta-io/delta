<img src="https://docs.delta.io/latest/_static/delta-lake-white.png" width="400" alt="Delta Lake Logo"></img>

[![CircleCI](https://circleci.com/gh/delta-io/delta/tree/master.svg?style=svg)](https://circleci.com/gh/delta-io/delta/tree/master)

Delta Lake is a storage layer that brings scalable, ACID transactions to [Apache Spark](https://spark.apache.org) and other big-data engines.

See the [Delta Lake Documentation](https://docs.delta.io) for details.

See the [Quick Start Guide](https://docs.delta.io/latest/quick-start.html) to get started with Scala, Java and Python.

## Latest Binaries

### Maven

You include Delta Lake in your Maven project by adding it as a dependency in your POM file. Delta Lake is cross compiled with Scala versions 2.11 and 2.12; choose the version that matches your project. If you are writing a Java project, you can use either version.

```xml
<dependency>
  <groupId>io.delta</groupId>
  <artifactId>delta-core_2.11</artifactId>
  <version>0.4.0</version>
</dependency>
```

### SBT

You include Delta Lake in your SBT project by adding the following line to your build.sbt file:

```scala
libraryDependencies += "io.delta" %% "delta-core" % "0.4.0"
```

## API Documentation

* [Scala API docs](https://docs.delta.io/latest/delta-apidoc.html)
* [Java API docs](https://docs.delta.io/latest/api/java/index.html)
* [Python API docs](https://docs.delta.io/latest/api/python/index.html)

## Compatibility

### Compatibility with Apache Spark Versions

Delta Lake currently requires Apache Spark 2.4.2. Earlier versions are missing [SPARK-27453](https://issues.apache.org/jira/browse/SPARK-27453), which breaks the `partitionBy` clause of the `DataFrameWriter`.

### API Compatibility

The only stable, public APIs currently provided by Delta Lake are through the `DataFrameReader`/`Writer` (i.e. `spark.read`, `df.write`, `spark.readStream` and `df.writeStream`). Options to these APIs will remain stable within a major release of Delta Lake (e.g., 1.x.x).

All other interfaces in the this library are considered internal, and they are subject to change across minor / patch releases.

### Data Storage Compatibility

Delta Lake guarantees backward compatibility for all Delta Lake tables (i.e., newer versions of Delta Lake will always be able to read tables written by older versions of Delta Lake). However, we reserve the right to break forwards compatibility as new features are introduced to the transaction protocol (i.e., an older version of Delta Lake may not be able to read a table produced by a newer version).

Breaking changes in the protocol are indicated by incrementing the minimum reader/writer version in the `Protocol` [action](https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala).

## Roadmap

Delta Lake is a recent open source project based on technology developed at Databricks. We plan to open-source all APIs that are required to correctly run Spark programs that read and write Delta tables. For a detailed timeline on this effort see the [project roadmap](https://github.com/delta-io/delta/milestones).

# Building

Delta Lake Core is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html). 

To compile, run

    build/sbt compile

To generate artifacts, run

    build/sbt package

To execute tests, run
  
    build/sbt test

Refer to [SBT docs](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html) for more commands.

# Transaction Protocol

[Delta Transaction Log Protocol](PROTOCOL.md) document provides a specification of the transaction protocol.

## Requirements for Underlying Storage Systems

Delta Lake ACID guarantees are predicated on the atomicity and durability guarantees of the storage system. Specifically, we require the storage system to provide the following. 

1. **Atomic visibility**: There must be a way for a file to be visible in its entirety or not visible at all. 
2. **Mutual exclusion**: Only one writer must be able to create (or rename) a file at the final destination.
3. **Consistent listing**: Once a file has been written in a directory, all future listings for that directory must return that file.

Given that storage systems do not necessarily provide all of these guarantees out-of-the-box, Delta Lake transactional operations typically go through the [LogStore API](https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/storage/LogStore.scala) instead of accessing the storage system directly. We can plug in custom `LogStore` implementations in order to provide the above guarantees for different storage systems. Delta Lake has built-in `LogStore` implementations for HDFS, Amazon S3 and Azure storage services. Please see [Delta Lake Storage Configuration](https://docs.delta.io/latest/delta-storage.html) for more details. If you are interested in adding a custom `LogStore` implementation for your storage system, you can start discussions in the community mailing group.

As an optimization, storage systems can also allow _partial listing of a directory, given a start marker_. Delta Lake can use this ability to efficiently discover the latest version of a table, without listing all of the files in the transaction log.

## Concurrency Control

Delta Lake ensures _serializability_ for concurrent reads and writes. Please see [Delta Lake Concurrency Control](https://docs.delta.io/latest/delta-concurrency.html) for more details.

# Reporting issues

We use [GitHub Issues](https://github.com/delta-io/delta/issues) to track community reported issues. You can also [contact](#community) the community for getting answers.

# Contributing 

We welcome contributions to Delta Lake. We use [GitHub Pull Requests ](https://github.com/delta-io/delta/pulls) for accepting changes. You will be prompted to sign a contributor license agreement before your change can be accepted.

# Community

There are two mediums of communication within the Delta Lake community. 

- Public Slack Channel
  - [Register here](https://join.slack.com/t/delta-users/shared_invite/enQtNTY1NDg0ODcxOTI1LWJkZGU3ZmQ3MjkzNmY2ZDM0NjNlYjE4MWIzYjg2OWM1OTBmMWIxZTllMjg3ZmJkNjIwZmE1ZTZkMmQ0OTk5ZjA)
  - [Login here](https://delta-users.slack.com/)

- Public [Mailing list](https://groups.google.com/forum/#!forum/delta-users)
