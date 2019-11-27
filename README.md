# <img src="https://docs.delta.io/latest/_static/delta-lake-white.png" width="150" alt="Delta Lake Logo"></img> Connectors

[![CircleCI](https://circleci.com/gh/delta-io/connectors/tree/master.svg?style=svg)](https://circleci.com/gh/delta-io/connectors/tree/master)

We are building connectors to bring [Delta Lake](https://delta.io) to popular big-data engines outside [Apache Spark](https://spark.apache.org) (e.g., [Apache Hive](https://hive.apache.org/), [Presto](https://prestodb.io/)).

# Introduction

This is the repository for Delta Lake Connectors. It includes a library for querying Delta Lake metadata and connectors to popular big-data engines (e.g., [Apache Hive](https://hive.apache.org/), [Presto](https://prestodb.io/)). Please refer to the main [Delta Lake](https://github.com/delta-io/delta) repository if you want to learn more about the Delta Lake project.

# Building

The project is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html). It has the following subprojects.

## 1. Delta uber jar

This project generates a single uber jar containing Delta Lake and all it transitive dependencies (except Hadoop and its dependencies).
- Most of the dependencies are shaded to avoid version conflicts. See the file build.sbt for details on what are not shaded.
- Hadoop and its dependencies is not included in the jar because they are expected to be present in the deployment environment.
- To generate the uber jar, run `build/sbt core/compile`
- To test the uber jar, run `build/sbt coreTest/test`

## 2. Hive connector
This project contains all the code needed to make Hive read Delta Lake tables.
- To compile the project, run `build/sbt hive/compile`
- To test the project, run `build/sbt hive/test`
- To generate the connector jar run `build/sbt hive/package`

Config `HIVE_AUX_JARS_PATH` in hive-env.sh with above two jars(uber jar and Hive connector jar)

Refer to [SBT docs](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html) for more commands.

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
