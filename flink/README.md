# Flink/Delta Connector

[![License](https://img.shields.io/badge/license-Apache%202-brightgreen.svg)](https://github.com/delta-io/connectors/blob/master/LICENSE.txt)

Official Delta Lake connector for [Apache Flink](https://flink.apache.org/).

## Introduction

Flink/Delta Connector is a JVM library to read and write data from Apache Flink applications to Delta tables
utilizing [Delta Standalone JVM library](https://github.com/delta-io/connectors#delta-standalone). It includes:

- `DeltaSink` for writing data from Apache Flink to a Delta table
- `DeltaSource` for reading Delta tables using Apache Flink (still in progress)

#### Note:

- `DeltaSink` provides exactly-once delivery guarantees.
- Depending on the version of the connector you can use it with following Apache Flink versions:
  
  | connector's version  | Flink's version |
  | :---: | :---: |
  |    0.4.0    |    >= 1.12.0    |
  
#### Known limitations:

- Currently only `DeltaSink` is supported, and thus the connector supports writing to Delta tables, but does not support reading Delta tables.
- The current version only supports Flink `Datastream` API. Support for Flink Table API / SQL, along with Flink Catalog's implementation for storing Delta table's metadata in an external metastore, are planned to be added in the next releases.
- The current version only provides Delta Lake's transactional guarantees for tables stored on HDFS and Microsoft Azure Storage.

## Java API docs
See the [Java API docs](https://delta-io.github.io/connectors/latest/delta-flink/api/java/index.html) here.

### Usage

You can add the Flink/Delta Connector library as a dependency using your favorite build tool. Please note
that it expects the following packages to be provided:

- `delta-standalone`
- `flink-parquet`
- `flink-table-common`
- `hadoop-client`

Please see the following build files for more details.

#### Maven

Scala 2.12:

```xml

<project>
    <properties>
        <scala.main.version>2.12</scala.main.version>
        <flink-version>1.12.0</flink-version>
        <hadoop-version>3.1.0</hadoop-version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>delta-flink</artifactId>
            <version>0.4.0</version>
        </dependency>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>delta-standalone_${scala.main.version}</artifactId>
            <version>0.4.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-parquet_${scala.main.version}</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop-version}</version>
        </dependency>
    </dependencies>
</project>
```

#### SBT

Please replace the versions of the dependencies with the ones you are using.

```
libraryDependencies ++= Seq(
  "io.delta" %% "delta-flink" % "0.4.0",
  "io.delta" %% "delta-standalone" % "0.4.0",  
  "org.apache.flink" %% "flink-parquet" % flinkVersion,
  "org.apache.flink" % "flink-table-common" % flinkVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion)
```

## Building

The project is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html).

### Environment requirements

- JDK 8 or above.
- Scala 2.11 or 2.12.

### Build commands

- To compile the project, run `build/sbt flink/compile`
- To test the project, run `build/sbt flink/test`
- To publish the JAR, run `build/sbt flink/publishM2`

## Examples
#### 1. Sink creation for non-partitioned tables

In this example we show how to create a `DeltaSink` and plug it to an
existing `org.apache.flink.streaming.api.datastream.DataStream`.

```java
package com.example;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

public class DeltaSinkExample {

    public DataStream<RowData> createDeltaSink(DataStream<RowData> stream,
                                               String deltaTablePath,
                                               RowType rowType) {
        DeltaSink<RowData> deltaSink = DeltaSink
            .forRowData(
                new Path(deltaTablePath),
                new Configuration(),
                rowType)
            .build();
        stream.sinkTo(deltaSink);
        return stream;
    }
}
```

#### 2. Sink creation for partitioned tables

In this example we show how to create a `DeltaSink` for `org.apache.flink.table.data.RowData` to
write data to a partitioned table using one partitioning column `surname`.

```java
package com.example;

import io.delta.flink.sink.DeltaBucketAssigner;
import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.DeltaSinkBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class DeltaSinkExample {

  public static final RowType ROW_TYPE = new RowType(Arrays.asList(
          new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH)),
          new RowType.RowField("surname", new VarCharType(VarCharType.MAX_LENGTH)),
          new RowType.RowField("age", new IntType())
  ));

  public DataStream<RowData> createDeltaSink(DataStream<RowData> stream,
                                             String deltaTablePath) {
    String[] partitionCols = {"surname"};
    DeltaSink<RowData> deltaSink = DeltaSink
            .forRowData(
                new Path(deltaTablePath),
                new Configuration(),
                rowType)
            .withPartitionColumns(partitionCols)
            .build();
    stream.sinkTo(deltaSink);
    return stream;
  }
}
```

## Frequently asked questions (FAQ)

#### Can I use this connector to read data from a Delta table?

No, currently we are supporting only writing to a Delta table. A `DeltaSource` API with the support for reading data from
Delta tables will be added in future releases.

#### Can I use this connector to append data to a Delta table?

Yes, you can use this connector to append data to either an existing or a new Delta table (if there is no existing
Delta log in a given path then it will be created by the connector).

#### Can I use this connector with other modes (overwrite, upsert etc.) ?

No, currently only append is supported. Other modes may be added in future releases.

#### Do I need to specify the partition columns when creating a Delta table?

If you'd like your data to be partitioned, then you should. If you are using the `DataStream API`, then
you can provide the partition columns using the `RowDataDeltaSinkBuilder.withPartitionColumns(List<String> partitionCols)` API.

#### Why do I need to specify the table schema? Shouldn’t it exist in the underlying Delta table metadata or be extracted from the stream's metadata?

Unfortunately we cannot extract schema information from a generic `DataStream`, and it is also required for interacting
with the Delta log. The sink must be aware of both Delta table's schema and the structure of the events in the stream in
order not to violate the integrity of the table.

#### What if I change the underlying Delta table schema ?

Next commit (after mentioned schema change) performed from the `DeltaSink` to the Delta log will fail unless you will
set `shouldTryUpdateSchema` param to true. In such case Delta Standalone will try to merge both schemas and check for
their compatibility. If this check fails (e.g. the change consisted of removing a column) the commit to the Delta Log will fail, which will cause failure of the Flink job.

## Local Development & Testing

- Before local debugging of `flink` tests in IntelliJ, run all `flink` tests using SBT. It will
  generate `Meta.java` object under your target directory that is providing the connector with correct version of the
  connector.
