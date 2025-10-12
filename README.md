<img src="https://docs.delta.io/latest/_static/delta-lake-white.png" width="200" alt="Delta Lake Logo"></img>

[![Test](https://github.com/delta-io/delta/actions/workflows/test.yaml/badge.svg)](https://github.com/delta-io/delta/actions/workflows/test.yaml)
[![License](https://img.shields.io/badge/license-Apache%202-brightgreen.svg)](https://github.com/delta-io/delta/blob/master/LICENSE.txt)
[![PyPI](https://img.shields.io/pypi/v/delta-spark.svg)](https://pypi.org/project/delta-spark/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/delta-spark)](https://pypistats.org/packages/delta-spark)

Delta Lake is an open-source storage framework that enables building a [Lakehouse architecture](http://cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf) with compute engines including Spark, PrestoDB, Flink, Trino, and Hive and APIs for Scala, Java, Rust, Ruby, and Python. 
* See the [Delta Lake Documentation](https://docs.delta.io) for details.
* See the [Quick Start Guide](https://docs.delta.io/latest/quick-start.html) to get started with Scala, Java and Python.
* Note, this repo is one of many Delta Lake repositories in the [delta.io](https://github.com/delta-io) organizations including
[delta](https://github.com/delta-io/delta), 
[delta-rs](https://github.com/delta-io/delta-rs),
[delta-sharing](https://github.com/delta-io/delta-sharing),
[kafka-delta-ingest](https://github.com/delta-io/kafka-delta-ingest), and
[website](https://github.com/delta-io/website).

The following are some of the more popular Delta Lake integrations, refer to [delta.io/integrations](https://delta.io/integrations/) for the complete list:

* [Apache Spark™](https://docs.delta.io/): This connector allows Apache Spark™ to read from and write to Delta Lake.
* [Apache Flink (Preview)](https://github.com/delta-io/delta/tree/master/connectors/flink): This connector allows Apache Flink to write to Delta Lake.
* [PrestoDB](https://prestodb.io/docs/current/connector/deltalake.html): This connector allows PrestoDB to read from Delta Lake.
* [Trino](https://trino.io/docs/current/connector/delta-lake.html): This connector allows Trino to read from and write to Delta Lake.
* [Delta Standalone](https://docs.delta.io/latest/delta-standalone.html): This library allows Scala and Java-based projects (including Apache Flink, Apache Hive, Apache Beam, and PrestoDB) to read from and write to Delta Lake.
* [Apache Hive](https://docs.delta.io/latest/hive-integration.html): This connector allows Apache Hive to read from Delta Lake.
* [Delta Rust API](https://docs.rs/deltalake/latest/deltalake/): This library allows Rust (with Python and Ruby bindings) low level access to Delta tables and is intended to be used with data processing frameworks like datafusion, ballista, rust-dataframe, vega, etc.

<br/>

<details>
<summary><strong><em>Table of Contents</em></strong></summary>

* [Latest binaries](#latest-binaries)
* [API Documentation](#api-documentation)
* [Compatibility](#compatibility)
  * [API Compatibility](#api-compatibility)
  * [Data Storage Compatibility](#data-storage-compatibility)
* [Roadmap](#roadmap)
* [Building](#building)
* [Transaction Protocol](#transaction-protocol)
* [Requirements for Underlying Storage Systems](#requirements-for-underlying-storage-systems)
* [Concurrency Control](#concurrency-control)
* [Reporting issues](#reporting-issues)
* [Contributing](#contributing)
* [License](#license)
* [Community](#community)
</details>


## Latest Binaries

See the [online documentation](https://docs.delta.io/latest/) for the latest release.

## API Documentation

* [Scala API docs](https://docs.delta.io/latest/delta-apidoc.html)
* [Java API docs](https://docs.delta.io/latest/api/java/index.html)
* [Python API docs](https://docs.delta.io/latest/api/python/index.html)

## Compatibility
[Delta Standalone](https://docs.delta.io/latest/delta-standalone.html) library is a single-node Java library that can be used to read from and write to Delta tables. Specifically, this library provides APIs to interact with a table’s metadata in the transaction log, implementing the Delta Transaction Log Protocol to achieve the transactional guarantees of the Delta Lake format.


### API Compatibility

There are two types of APIs provided by the Delta Lake project. 

- Direct Java/Scala/Python APIs - The classes and methods documented in the [API docs](https://docs.delta.io/latest/delta-apidoc.html) are considered as stable public APIs. All other classes, interfaces, methods that may be directly accessible in code are considered internal, and they are subject to change across releases.
- Spark-based APIs - You can read Delta tables through the `DataFrameReader`/`Writer` (i.e. `spark.read`, `df.write`, `spark.readStream` and `df.writeStream`). Options to these APIs will remain stable within a major release of Delta Lake (e.g., 1.x.x).
- See the [online documentation](https://docs.delta.io/latest/releases.html) for the releases and their compatibility with Apache Spark versions.


### Data Storage Compatibility

Delta Lake guarantees backward compatibility for all Delta Lake tables (i.e., newer versions of Delta Lake will always be able to read tables written by older versions of Delta Lake). However, we reserve the right to break forward compatibility as new features are introduced to the transaction protocol (i.e., an older version of Delta Lake may not be able to read a table produced by a newer version).

Breaking changes in the protocol are indicated by incrementing the minimum reader/writer version in the `Protocol` [action](https://github.com/delta-io/delta/blob/master/spark/src/test/scala/org/apache/spark/sql/delta/ActionSerializerSuite.scala).

## Roadmap

* For the high-level Delta Lake roadmap, see [Delta Lake 2022H1 roadmap](http://delta.io/roadmap).  
* For the detailed timeline, see the [project roadmap](https://github.com/delta-io/delta/milestones). 

## Transaction Protocol

[Delta Transaction Log Protocol](PROTOCOL.md) document provides a specification of the transaction protocol.

## Requirements for Underlying Storage Systems

Delta Lake ACID guarantees are predicated on the atomicity and durability guarantees of the storage system. Specifically, we require the storage system to provide the following.

1. **Atomic visibility**: There must be a way for a file to be visible in its entirety or not visible at all.
2. **Mutual exclusion**: Only one writer must be able to create (or rename) a file at the final destination.
3. **Consistent listing**: Once a file has been written in a directory, all future listings for that directory must return that file.

See the [online documentation on Storage Configuration](https://docs.delta.io/latest/delta-storage.html) for details.

## Concurrency Control

Delta Lake ensures _serializability_ for concurrent reads and writes. Please see [Delta Lake Concurrency Control](https://docs.delta.io/latest/delta-concurrency.html) for more details.

## Reporting issues

We use [GitHub Issues](https://github.com/delta-io/delta/issues) to track community reported issues. You can also [contact](#community) the community for getting answers.

## Contributing 

We welcome contributions to Delta Lake. See our [CONTRIBUTING.md](https://github.com/delta-io/delta/blob/master/CONTRIBUTING.md) for more details.

We also adhere to the [Delta Lake Code of Conduct](https://github.com/delta-io/delta/blob/master/CODE_OF_CONDUCT.md).

## Building

Delta Lake is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html).

To compile, run

    build/sbt compile

To generate artifacts, run

    build/sbt package

To execute tests, run

    build/sbt test

To execute a single test suite, run

    build/sbt spark/'testOnly org.apache.spark.sql.delta.optimize.OptimizeCompactionSQLSuite'

To execute a single test within and a single test suite, run

    build/sbt spark/'testOnly *.OptimizeCompactionSQLSuite -- -z "optimize command: on partitioned table - all partitions"'

Refer to [SBT docs](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html) for more commands.

## Running python tests locally

### Setup Environment
#### Install Conda (Skip if you already installed it)
Follow [Conda Download](https://www.anaconda.com/download/) to install Anaconda.

#### Create an environment from environment file
Follow [Create Environment From Environment file](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-from-file) to create a Conda environment from `<repo-root>/python/environment.yml` and activate the newly created `delta_python_tests` environment.

```
# Note the `--file` argument should be a fully qualified path. Using `~` in file
# path doesn't work. Example valid path: `/Users/macuser/delta/python/environment.yml`

conda env create --name delta_python_tests --file=<absolute_path_to_delta_repo>/python/environment.yml`
```

#### JDK Setup
Build needs JDK 1.8. Make sure to setup `JAVA_HOME` that points to JDK 1.8.

#### Running tests
```
conda activate delta_python_tests
python3 <delta-root>/python/run-tests.py
```

## IntelliJ Setup

IntelliJ is the recommended IDE to use when developing Delta Lake. To import Delta Lake as a new project:
1. Clone Delta Lake into, for example, `~/delta`.
2. In IntelliJ, select `File` > `New Project` > `Project from Existing Sources...` and select `~/delta`.
3. Under `Import project from external model` select `sbt`. Click `Next`.
4. Under `Project JDK` specify a valid Java `1.8` JDK and opt to use SBT shell for `project reload` and `builds`.
5. Click `Finish`.
6. In your terminal, run `build/sbt clean package`. Make sure you use Java `1.8`. The build will generate files 
   that are necessary for Intellij to index the repository.

### Setup Verification

After waiting for IntelliJ to index, verify your setup by running a test suite in IntelliJ.
1. Search for and open `DeltaLogSuite`
2. Next to the class declaration, right click on the two green arrows and select `Run 'DeltaLogSuite'`

### Troubleshooting

If you see errors of the form

```
Error:(46, 28) object DeltaSqlBaseParser is not a member of package io.delta.sql.parser
import io.delta.sql.parser.DeltaSqlBaseParser._
...
Error:(91, 22) not found: type DeltaSqlBaseParser
    val parser = new DeltaSqlBaseParser(tokenStream)
```

then follow these steps:
1. Ensure you are using Java `1.8`. You can set this using
```
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`
```
2. Compile using the SBT CLI: `build/sbt clean compile`.
2. Go to `File` > `Project Structure...` > `Modules` > `delta-spark`.
3. In the right panel under `Source Folders` remove any `target` folders, e.g. `target/scala-2.12/src_managed/main [generated]`
4. Click `Apply` and then re-run your test.

## License
Apache License 2.0, see [LICENSE](https://github.com/delta-io/delta/blob/master/LICENSE.txt).

## Community

There are two mediums of communication within the Delta Lake community.

* Public Slack Channel
  - [Register here](https://go.delta.io/slack)
  - [Login here](https://delta-users.slack.com/)
* [Linkedin page](https://www.linkedin.com/company/deltalake)
* [Youtube channel](https://www.youtube.com/c/deltalake)
* Public [Mailing list](https://groups.google.com/forum/#!forum/delta-users)
