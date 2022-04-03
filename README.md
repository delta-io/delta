<img src="https://docs.delta.io/latest/_static/delta-lake-white.png" width="400" alt="Delta Lake Logo"></img>

[![Test](https://github.com/delta-io/delta/actions/workflows/test.yaml/badge.svg)](https://github.com/delta-io/delta/actions/workflows/test.yaml)
[![License](https://img.shields.io/badge/license-Apache%202-brightgreen.svg)](https://github.com/delta-io/delta/blob/master/LICENSE.txt)
[![PyPI](https://img.shields.io/pypi/v/delta-spark.svg)](https://pypi.org/project/delta-spark/)

Delta Lake is an open-source storage framework that enables building a [Lakehouse architecture](http://cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf) with compute engines including Spark, PrestoDB, Flink, Trino, and Hive and APIs for Scala, Java, Rust, Ruby, and Python. See the [Delta Lake Documentation](https://docs.delta.io) for details.

See the [Quick Start Guide](https://docs.delta.io/latest/quick-start.html) to get started with Scala, Java and Python.

## Latest Binaries

See the [online documentation](https://docs.delta.io/latest/) for the latest release.

## API Documentation

* [Scala API docs](https://docs.delta.io/latest/delta-apidoc.html)
* [Java API docs](https://docs.delta.io/latest/api/java/index.html)
* [Python API docs](https://docs.delta.io/latest/api/python/index.html)

## Compatibility
[Delta Standalone](https://www.youtube.com/c/deltalake) library is a single-node Java library that can be used to read from and write to Delta tables. Specifically, this library provides APIs to interact with a table’s metadata in the transaction log, implementing the Delta Transaction Log Protocol to achieve the transactional guarantees of the Delta Lake format.

### Integrations with Delta Lake:
See the online documentation for the Delta lake integration with big data ecosystem.

* [Apache Spark](https://docs.delta.io/latest/releases.html)
* [PrestoDb](https://github.com/prestodb/presto/tree/master/presto-delta)
* [Trino](https://trino.io/docs/current/connector/delta-lake.html)
* [Rust API](https://github.com/delta-io/delta-rs)
* [Kafka Delta Ingest](https://github.com/delta-io/kafka-delta-ingest)
* [Apache Hive](https://github.com/delta-io/connectors/tree/master/hive)
* [Apache Flink](https://github.com/delta-io/connectors/tree/master/flink)

### API Compatibility

There are two types of APIs provided by the Delta Lake project. 

- Direct Java/Scala/Python APIs - The classes and methods documented in the [API docs](https://docs.delta.io/latest/delta-apidoc.html) are considered as stable public APIs. All other classes, interfaces, methods that may be directly accessible in code are considered internal, and they are subject to change across releases.
- Spark-based APIs - You can read Delta tables through the `DataFrameReader`/`Writer` (i.e. `spark.read`, `df.write`, `spark.readStream` and `df.writeStream`). Options to these APIs will remain stable within a major release of Delta Lake (e.g., 1.x.x).


### Data Storage Compatibility

Delta Lake guarantees backward compatibility for all Delta Lake tables (i.e., newer versions of Delta Lake will always be able to read tables written by older versions of Delta Lake). However, we reserve the right to break forward compatibility as new features are introduced to the transaction protocol (i.e., an older version of Delta Lake may not be able to read a table produced by a newer version).

Breaking changes in the protocol are indicated by incrementing the minimum reader/writer version in the `Protocol` [action](https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala).

## Roadmap

For detailed detailed timeline, see the [project roadmap](https://github.com/delta-io/delta/milestones).

# Building

Delta Lake is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html).

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

See the [online documentation on Storage Configuration](https://docs.delta.io/latest/delta-storage.html) for details.

## Concurrency Control

Delta Lake ensures _serializability_ for concurrent reads and writes. Please see [Delta Lake Concurrency Control](https://docs.delta.io/latest/delta-concurrency.html) for more details.

# Reporting issues

We use [GitHub Issues](https://github.com/delta-io/delta/issues) to track community reported issues. You can also [contact](#community) the community for getting answers.

# Contributing 
We welcome contributions to Delta Lake. See our [CONTRIBUTING.md](https://github.com/delta-io/delta/blob/master/CONTRIBUTING.md) for more details.

We also adhere to the [Delta Lake Code of Conduct](https://github.com/delta-io/delta/blob/master/CODE_OF_CONDUCT.md).

# License
Apache License 2.0, see [LICENSE](https://github.com/delta-io/delta/blob/master/LICENSE.txt).

# Community

There are two mediums of communication within the Delta Lake community.

* Public Slack Channel
  - [Register here](https://dbricks.co/delta-users-slack)
  - [Login here](https://delta-users.slack.com/)
* Public [Mailing list](https://groups.google.com/forum/#!forum/delta-users)
* [Linkedin page](https://www.linkedin.com/company/deltalake)
* [Youtube channel](https://www.youtube.com/c/deltalake)
