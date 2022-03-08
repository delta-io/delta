# Flink Delta Lake Connector

[![License](https://img.shields.io/badge/license-Apache%202-brightgreen.svg)](https://github.com/delta-io/connectors/blob/master/LICENSE.txt)

Official Delta Lake connector for [Apache Flink](https://flink.apache.org/).

# Introduction

Flink Delta Lake Connector is a JVM library to read and write data from Apache Flink applications to Delta Lake tables
utilizing [Delta Standalone JVM library](https://github.com/delta-io/connectors#delta-standalone). It includes

- Sink for writing data from Apache Flink to a Delta table
- Source for reading Delta Lake's table using Apache Flink (still in progress)

NOTE:

- currently only sink is supported which means that this connectors supports only writing to a Delta table and is not
  able to read from it
- Flink DeltaSink provides exactly-once delivery guarantees
- depending on the version of the connector you can use it with following Apache Flink versions:
  
  | connector's version  | Flink's version |
  | :---: | :---: |
  |    0.2.1-SNAPSHOT    |    >= 1.12.0    |

### Usage

You can add the Flink Connector library as a dependency using your favorite build tool. Please note
that it expects packages:

- `delta-standalone`
- `flink-parquet`
- `flink-table-common`
- `hadoop-client`

to be provided. Please see the following build files for more details.

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
            <artifactId>flink-connector</artifactId>
            <version>0.2.1-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>delta-standalone_${scala.main.version}</artifactId>
            <version>0.2.1-SNAPSHOT</version>
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
  "io.delta" %% "flink-connector" % "0.2.1-SNAPSHOT",
  "io.delta" %% "delta-standalone" % "0.2.1-SNAPSHOT",  
  "org.apache.flink" %% "flink-parquet" % flinkVersion,
  "org.apache.flink" % "flink-table-common" % flinkVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion)
```

# Building

The project is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html).

### Environment Requirements

- JDK 8 or above.
- Scala 2.11 or 2.12.

### Build commands

- To compile the project, run `build/sbt flinkConnector/compile`
- To test the project, run `build/sbt flinkConnector/test`
- To publish the JAR, run `build/sbt flinkConnector/publishM2`

## Examples
#### 1. Sink Creation for non-partitioned tables

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

#### 2. Sink Creation for partitioned tables

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
    List<String> partitionCols = Arrays.asList("surname");
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

### Frequently asked questions (FAQ)

#### Can I use this connector to read data from a Delta Lake table?

No, currently we are supporting only writing to a Delta Lake table. A `DeltaSource` API with the support for reading data from
Delta tables will be added in future releases.

#### Can I use this connector to append data to a Delta Lake table?

Yes, you can use this connector to append data to either an existing or a new Delta Lake table (if there is no existing
Delta Log in a given path then it will be created by the connector).

#### Can I use this connector with other modes (overwrite, upsert etc.) ?

No, currently only append is supported. Other modes may be added in future releases.

#### Do I need to specify the partition columns when creating a Delta table?

If you'd like your data to be partitioned, then you should. If you are using the DataStream API, then
you can provide the partition columns using the `RowDataDeltaSinkBuilder.withPartitionColumns(List<String> partitionCols` API.

#### Why do I need to specify the table schema? Shouldn’t it exist in the underlying Delta table metadata or be extracted from the stream's metadata?

Unfortunately we cannot extract schema information from a generic DataStream, and it is also required for interacting
with DeltaLog. The sink must be aware of both Delta table's schema and the structure of the events in the stream in
order not to violate the integrity of the table.

#### What if I change the underlying Delta table schema ?

Next commit (after mentioned schema change) performed from the DeltaSink to a DeltaLog will fail unless you will
set `shouldTryUpdateSchema` param to true. In such case DeltaStandalone will try to merge both schemas and check for
their compatibility. If this check will fail (e.g. the change consisted of removing a column) so will the DeltaLog's
commit which will cause failure of the Flink job.

## Local Development & Testing

- Before local debugging of `flink-connector` tests in IntelliJ, run all `flink-connectors` tests using SBT. It will
  generate `Meta.java` object under your target directory that is providing the connector with correct version of the
  connector.
