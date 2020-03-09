# <img src="https://docs.delta.io/latest/_static/delta-lake-white.png" width="150" alt="Delta Lake Logo"></img> Connectors

[![CircleCI](https://circleci.com/gh/delta-io/connectors/tree/master.svg?style=svg)](https://circleci.com/gh/delta-io/connectors/tree/master)

We are building connectors to bring [Delta Lake](https://delta.io) to popular big-data engines outside [Apache Spark](https://spark.apache.org) (e.g., [Apache Hive](https://hive.apache.org/), [Presto](https://prestodb.io/)).

# Introduction

This is the repository for Delta Lake Connectors. It includes a library for querying Delta Lake metadata and connectors to popular big-data engines (e.g., [Apache Hive](https://hive.apache.org/), [Presto](https://prestodb.io/)). Please refer to the main [Delta Lake](https://github.com/delta-io/delta) repository if you want to learn more about the Delta Lake project.

# Building

The project is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html). It has the following subprojects.

## Delta uber jar

This project generates a single uber jar containing Delta Lake and all it transitive dependencies (except Hadoop and its dependencies).
- Most of the dependencies are shaded to avoid version conflicts. See the file build.sbt for details on what are not shaded.
- Hadoop and its dependencies is not included in the jar because they are expected to be present in the deployment environment.
- To generate the uber jar, run `build/sbt core/compile`
- To test the uber jar, run `build/sbt coreTest/test`

## Hive connector
This project contains all the code needed to make Hive read Delta Lake tables.
- To compile the project, run `build/sbt hive/compile`
- To test the project, run `build/sbt hive/test`
- To generate the connector jar run `build/sbt hive/package`

The above commands will generate two JARs in the following paths.

```
core/target/scala-2.12/delta-core-shaded-assembly_2.12-0.5.0.jar
hive/target/scala-2.12/hive-delta_2.12-0.5.0.jar
```

These two JARs include the Hive connector and all its dependencies. They need to be put in Hive’s classpath.

Note: if you would like to build jars using Scala 2.11, you can run the SBT command `build/sbt "++ 2.11.12 hive/package"` and the generated JARS will be in the following paths.

```
core/target/scala-2.11/delta-core-shaded-assembly_2.11-0.5.0.jar
hive/target/scala-2.12/hive-delta_2.11-0.5.0.jar
```

### Setting up Hive

This sections describes how to set up Hive to load the Delta Hive connector.

Before starting your Hive CLI or running your Hive script, add the following special Hive config to the `hive-site.xml` file (Its location is `/etc/hive/conf/hive-site.xml` in a EMR cluster).

```xml
<property>
  <name>hive.input.format</name>
  <value>io.delta.hive.HiveInputFormat</value>
</property>
```

The second step is to upload the above two JARs to the machine that runs Hive. Finally, add the paths of the JARs toHive’s environment variable, `HIVE_AUX_JARS_PATH`. You can find this environment variable in the `hive-env.sh` file, whose location is `/etc/hive/conf/hive-env.sh` on an EMR cluster. This setting will tell Hive where to find the connector JARs.

### Create a Hive table

After finishing setup, you should be able to create a Delta table in Hive.

Right now the connector supports only EXTERNAL Hive tables. The Delta table must be created using Spark before an external Hive table can reference it.

Here is an example of a CREATE TABLE command that defines an external Hive table pointing to a Delta table on `s3://foo-bucket/bar-dir`.

```SQL
CREATE EXTERNAL TABLE deltaTbl(a INT, b STRING)
STORED BY 'io.delta.hive.DeltaStorageHandler'
LOCATION 's3://foo-bucket/bar-dir’
```

`io.delta.hive.DeltaStorageHandler` is the class that implements Hive data source APIs. It will know how to load a Delta table and extract its metadata. The table schema in the `CREATE TABLE` statement must be consistent with the underlying Delta metadata. Otherwise, the connector will throw an error to tell you about the inconsistency.

### FAQs

#### Supported Hive versions
Hive 2.x. Please report any incompatible issues.

#### Do I need to specify the partition columns when creating a Delta table?
No. The partition columns are read from the underlying Delta metadata. The connector will know the partition columns and use this information to do the partition pruning automatically.

#### Why do I need to specify the table schema? Shouldn’t it exist in the underlying Delta table metadata?
Unfortunately, the table schema is a core concept of Hive and Hive needs it before calling the connector.

#### What if I change the underlying Delta table schema in Spark after creating the Hive table?
If the schema in the underlying Delta metadata is not consistent with the schema specified by `CREATE TABLE` statement, the connector will report an error when loading the table and ask you to fix the schema. You must drop the table and recreate it using the new schema. Hive 3.x exposes a new API to allow a data source to hook ALTER TABLE. You will be able to use ALTER TABLE to update a table schema when the connector supports Hive 3.x.

# Reporting issues

We use [GitHub Issues](https://github.com/delta-io/connectors/issues) to track community reported issues. You can also [contact](#community) the community for getting answers.

# Contributing

We welcome contributions to Delta Lake Connectors repository. We use [GitHub Pull Requests](https://github.com/delta-io/connectors/pulls) for accepting changes.

# Community

There are two mediums of communication within the Delta Lake community.

- Public Slack Channel
  - [Register here](https://join.slack.com/t/delta-users/shared_invite/enQtNTY1NDg0ODcxOTI1LWJkZGU3ZmQ3MjkzNmY2ZDM0NjNlYjE4MWIzYjg2OWM1OTBmMWIxZTllMjg3ZmJkNjIwZmE1ZTZkMmQ0OTk5ZjA)
  - [Login here](https://delta-users.slack.com/)

- Public [Mailing list](https://groups.google.com/forum/#!forum/delta-users)
