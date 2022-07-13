# <img src="https://docs.delta.io/latest/_static/delta-lake-white.png" width="150" alt="Delta Lake Logo"></img> Connectors

[![Test](https://github.com/delta-io/connectors/actions/workflows/test.yaml/badge.svg)](https://github.com/delta-io/connectors/actions/workflows/test.yaml)
[![License](https://img.shields.io/badge/license-Apache%202-brightgreen.svg)](https://github.com/delta-io/connectors/blob/master/LICENSE.txt)

We are building connectors to bring [Delta Lake](https://delta.io) to popular big-data engines outside [Apache Spark](https://spark.apache.org) (e.g., [Apache Hive](https://hive.apache.org/), [Presto](https://prestodb.io/), [Apache Flink](https://flink.apache.org/)) and also to common reporting tools like [Microsoft Power BI](https://powerbi.microsoft.com/).

# Introduction

This is the repository for Delta Lake Connectors. It includes
- [Delta Standalone](https://docs.delta.io/latest/delta-standalone.html): a native library for reading and writing Delta Lake metadata.
- Connectors to popular big-data engines (e.g., [Apache Hive](https://hive.apache.org/), [Presto](https://prestodb.io/), [Apache Flink](https://flink.apache.org/)) and to common reporting tools like [Microsoft Power BI](https://powerbi.microsoft.com/).

Please refer to the main [Delta Lake](https://github.com/delta-io/delta) repository if you want to learn more about the Delta Lake project.

# API documentation

- Delta Standalone [Java API docs](https://delta-io.github.io/connectors/latest/delta-standalone/api/java/index.html)
- Flink/Delta Connector [Java API docs](https://delta-io.github.io/connectors/latest/delta-flink/api/java/index.html)

# Delta Standalone

Delta Standalone, formerly known as the Delta Standalone Reader (DSR), is a JVM library to read **and write** Delta tables. Unlike https://github.com/delta-io/delta, this project doesn't use Spark to read or write tables and it has only a few transitive dependencies. It can be used by any application that cannot use a Spark cluster.
- To compile the project, run `build/sbt standalone/compile`
- To test the project, run `build/sbt standalone/test`
- To publish the JAR, run `build/sbt standaloneCosmetic/publishM2`

See [Delta Standalone](https://docs.delta.io/latest/delta-standalone.html) for detailed documentation.


# Connectors

## Hive Connector

Read Delta tables directly from Apache Hive using the [Hive Connector](/hive/README.md). See the dedicated [README.md](/hive/README.md) for more details.

## Flink/Delta Connector

Use the [Flink/Delta Connector](flink/README.md) to read and write Delta tables from Apache Flink applications. The connector includes a sink for writing to Delta tables from Apache Flink, and a source for reading Delta tables using Apache Flink (still in progress.) See the dedicated [README.md](/flink/README.md) for more details.

## sql-delta-import

[sql-delta-import](/sql-delta-import/readme.md) allows for importing data from a JDBC source into a Delta table.

## Power BI connector
The connector for [Microsoft Power BI](https://powerbi.microsoft.com/) is basically just a custom Power Query function that allows you to read a Delta table from any file-based [data source supported by Microsoft Power BI](https://docs.microsoft.com/en-us/power-bi/connect-data/desktop-data-sources). Details can be found in the dedicated [README.md](/powerbi/README.md).

# Reporting issues

We use [GitHub Issues](https://github.com/delta-io/connectors/issues) to track community reported issues. You can also [contact](#community) the community for getting answers.

# Contributing

We welcome contributions to Delta Lake Connectors repository. We use [GitHub Pull Requests](https://github.com/delta-io/connectors/pulls) for accepting changes.

# Community

There are two mediums of communication within the Delta Lake community.

- Public Slack Channel
  - [Register here](https://go.delta.io/slack)
  - [Login here](https://delta-users.slack.com/)

- Public [Mailing list](https://groups.google.com/forum/#!forum/delta-users)

# Local Development & Testing
- Before local debugging of `standalone` tests in IntelliJ, run all `standalone` tests using SBT. This helps IntelliJ recognize the golden tables as class resources.
