# TODO
- Complete the intro
- Add circle ci badge
- Update Linking section
- Add time travel code snippets
- Verify all the links everywhere



# Delta Lake Core

Delta Lake Core is .... (copy text from delta docs)

See the [Delta Lake Documentation](https://docs.delta.io) for more details.

# Usage Guide

## Linking
To use Delta Lake, you will have to create a new project with Maven Delta Lake's Maven coordinates.

Scala 2.11:

    groupId: io.delta
    artifactId: delta-core-2.11
    version: 0.1.0

Scala 2.12

    groupId: io.delta
    artifactId: delta-core-2.12
    version: 0.1.0

## Reading and Write to Delta Lake tables from using Apache Spark

You can write a to Delta Lake tables using standard Apache Spark DataFrame APIs. 

#### Scala API

    import org.apache.spark.sql.SparkSession

    val spark: SparkSession = ...  // create SparkSession

    // Writing to a Delta Lake table from a batch job
    dataframe.write
      .format("delta")
      .mode("overwrite")
      .save("pathToDeltaTable")

    // Reading from a Delta Lake table in a batch job 
    val dataframe = spark.read
      .format("delta")
      .load("pathToDeltaTable")

    // Writing to a Delta lake table from a streaming job
    streamingDataFrame.writeStream
      .format("delta")
      .start("pathToDeltaTable")

    // Writing to a Delta lake table from a streaming job
    val streamingDataFrame = spark.readStream
      .format("delta")
      .load("pathToDeltaTable")

#### Java API

    import org.apache.spark.sql.SparkSession;
    import org.apache.spark.sql.Dataset;
    import org.apache.spark.sql.Row;

    SparkSession spark = ...   // create SparkSession

    // Writing to a Delta Lake table from a batch job
    dataframe.write()
      .format("delta")
      .mode("overwrite")
      .save("pathToDeltaTable");

    // Reading from a Delta Lake table in a batch job 
    Dataset<Row> dataframe = spark.read()
      .format("delta")
      .load("pathToDeltaTable");

    // Writing to a Delta lake table from a streaming job
    streamingDataFrame.writeStream()
      .format("delta")
      .start("pathToDeltaTable");

    // Writing to a Delta lake table from a streaming job
    Dataset<Row> streamingDataFrame = spark.readStream()
      .format("delta")
      .load("pathToDeltaTable");


#### Python API

    from pyspark.sql import SparkSession

    spark = ... # create SparkSession

    # Writing to a Delta Lake table from a batch job
    dataframe.write \
      .format("delta") \
      .mode("overwrite") \
      .save("pathToDeltaTable")

    # Reading from a Delta Lake table in a batch job 
    dataframe = spark.read \
      .format("delta") \
      .load("pathToDeltaTable") \

    # Writing to a Delta lake table from a streaming job
    streamingDataFrame.writeStream  \
      .format("delta") \
      .start("pathToDeltaTable")

    # Writing to a Delta lake table from a streaming job
    streamingDataFrame = spark.readStream \
      .format("delta") \
      .load("pathToDeltaTable")

## Compatibility

This section states the compatibility guarantees provided by the current version of Delta Lake.

### Compatibility with Spark Versions

Current version of Delta Lake depends on SNAPSHOT build of Apache Spark 2.4 (nightly snapshot after XXXX). This is because Delta Lake requires the changes made by [SPARK-27453](https://issues.apache.org/jira/browse/SPARK-27453) for table partitioning to work and these changes are not yet available in an official Apache release. There will be new release of Delta Lake that depends on an official Spark version as soon as there is an Apache Spark release.

### Compatibility with storage systems

Delta Lake stores the transaction log of a table in the same storage system as the table. Hence, Delta Lake's ACID guarantees are predicated on the atomicity and durability guarantees of the storage system. Specifically, it requires the storage system to provide the following. 

1. Atomic visibility of files: There must a way for a file to visible in its entirely or not visible at all. 
2. Consistent listing: Once a file has been written in a directory, all future listings for that directory must return that file.

Open source Delta Lake currently supports all these guarantees only on HDFS. It is possible to make it work with other storage systems by plugging in custom implementations of the [LogStore API](XXX). However, [Managed Delta Lake](XXX) support AWS S3 and Azure Blob Stores.

### API Compatibility

Delta Lake guarantees compatibility with the Apache Spark DataFrameReader/Writer APIs, that is, `df.read`, `df.write`, `df.readStream` and `df.writeStream`. The interfaces of the implementation are currently considered internal and may change across minor versions.

### Data Compatibility

Delta Lake guarantees backward compatibility for all Delta Lake tables, that is, newer versions of Delta Lake will always be able to read tables written by older versions of Delta Lake. However, we reserve the right to break future compatibility, that is, older versions of Delta Lake may not be able to read tables written by newer version of Delta Lake. This is because we may add features that are usable only in newer versions. Users attempting to read new version tables from old version of Delta Lake should get a clear error message when forward compatibility is broken.

# Building

Delta Lake Core is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html). 

To compile, run

    build/sbt compile

To generate artifacts, run

    build/sbt package

To execute tests, run
  
    build/sbt test

Refer to [SBT docs](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html) for more commands.

# Reporting issues
We use [Github Issues](/../../issues/) to track community reported issues. You can also [contact](#community) the community for getting answers.

# Contributing 
We happily welcome contributions to Delta Lake. We use [Github Pull Requests ](/../../pulls/) for accepting changes.

# Community

There are two mediums of communication withing the Delta Lake community. 

- Public Slack Channel
  - [Register here](https://join.slack.com/t/delta-users/shared_invite/enQtNTY1NDg0ODcxOTI1LWE3YjMxOTM4MmM0YWNhNjE2YmI2OGI4N2Y3MTRhOWQ1YzE3MTMyYTM5YzRiZWZlYzMwYzk0M2JiZmJhY2Q4NWI)
  - [Login here](https://delta-users.slack.com/)

- Public [Mailing list](https://groups.google.com/forum/#!forum/delta-users)

