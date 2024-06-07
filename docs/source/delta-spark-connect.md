---
description: Learn about Delta Connect - Spark Connect in Delta.
---

# Delta Connect (or Spark Connect in Delta)

.. note:: This feature is available in preview in <Delta> 4.0 and above.

Delta Connect adds [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) support to Scala and Python APIs of Delta Lake for Apache Spark. Spark Connect is a new project released in Apache Spark 4.0 that adds a decoupled client-server infrastructure which allows remote connectivity from Spark from everywhere. Delta Connect makes the `DeltaTable` interfaces compatible with the new Spark Connect protocol.

# Motivation

Delta Connect is expected to bring the same benefits as Spark Connect:

1. Easier upgrading to more recent versions of Spark and Delta, as the client interface is completely decoupled from the server.
2. Simpler integration of Spark and Delta with developer tooling. IDEs no longer have to integrate with the full Spark and Delta implementation, and instead can integrate with a thin-client.
3. Support for languages other than Java/Scala and Python. Clients "merely" have to generate Protocol Buffers and therefore become simpler to implement.
4. Spark and Delta will become more stable, as user code is no longer running in the same JVM as Spark's driver.
5. Remote connectivity. Code can run anywhere now, as there is a gRPC layer between the user interface and the driver.

# Supported Delta Connect operations

The feature introduces a limited set of supported operations in <Delta> 4.0 preview and expands it in <Delta> 4.0 and above.

.. csv-table::
:header: "Supported operations - Delta 4.0 preview", "Supported operations - Delta 4.0 release"

"`forPath`","`forPath`"
"`forName`","`forName`"
"`alias`","`alias`"
"`toDF`","`toDF`"
,"`generate`"
,"`vacuum`"
,"`delete`"
,"`update`"
,"`merge`"
,"`history`"
,"`detail`"
,"`convertToDelta`"
,"`create`"
,"`createIfNotExists`"
,"`replace`"
,"`createOrReplace`"
,"`isDeltaTable`"
,"`upgradeTableProtocol`"
,"`restoreToVersion`"
,"`restoreToTimestamp`"
,"`optimize`"
,"`clone`"

# How to start the Delta Connect Server

1. Download `spark-4.0.0-preview1-bin-hadoop3.tgz` from [Spark 4.0.0 First Preview](https://archive.apache.org/dist/spark/spark-4.0.0-preview1).

2. Start the Spark Connect server with the Delta Connect plugins:

```bash
sbin/start-connect-server.sh \ 
  --packages org.apache.spark:spark-connect_2.13:4.0.0-preview1,io.delta:delta-connect-server_2.13:4.0.0-preview1,io.delta:delta-connect-common_2.13:4.0.0-preview1,com.google.protobuf:protobuf-java:3.25.1 \ 
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.connect.extensions.relation.classes=org.apache.spark.sql.connect.delta.DeltaRelationPlugin" \
  --conf "spark.connect.extensions.command.classes=org.apache.spark.sql.connect.delta.DeltaCommandPlugin"
```

# How to use the Delta Connect Python Client

The Delta Connect Python client is included in the same PyPi package as Delta Spark.

1. `pip install pyspark==4.0.0`

2. `pip install delta-spark==4.0.0`

3. There is no difference in usage compared to the classic way. We just need to pass in a remote `SparkSession` (instead of a local one) to the `DeltaTable` API.

An usage example:

```python
import io.delta.tables._
import org.apache.spark.sql.functions._

val deltaTable = DeltaTable.forName(spark, "my_table")

deltaTable.update(
  condition = expr("id % 2 == 0"),
  set = Map("id" -> expr("id + 100")))
```

# How to use the Delta Connect Scala Client

Make sure you are using Java 17!

1. `export SPARK_REMOTE="sc://localhost:15002"`

2. `export _JAVA_OPTIONS="--add-opens=java.base/ALL-UNNAMED"`

3. Download and install the [Coursier CLI](https://get-coursier.io/docs/cli-installation)

4. Start the Spark Connect client version 4.0.0 First Preview:

```bash
cs launch --jvm zulu:17.0.11 --scala 2.13.13 -r \
m2Local com.lihaoyi:::ammonite:3.0.0-M2 org.apache.spark::spark-connect-client-jvm:4.0.0-preview1 io.delta:delta-connect-client_2.13:4.0.0-preview1 io.delta:delta-connect-common_2.13:4.0.0-preview1 org.tpolecat::typename::1.1.0 com.google.protobuf:protobuf-java:3.25.1 \
--java-opt --add-opens=java.base/java.nio=ALL-UNNAMED \
-M org.apache.spark.sql.application.ConnectRep
```

An usage example:
    
```scala
import io.delta.tables._
import org.apache.spark.sql.functions._

val deltaTable = DeltaTable.forName(spark, "my_table")

deltaTable.update(
condition = expr("id % 2 == 0"),
set = Map("id" -> expr("id + 100")))
```

In the future, when [spark-connect-repl](https://spark.apache.org/docs/4.0.0-preview1/spark-connect-overview.html#use-spark-connect-for-interactive-analysis) moves to Spark 4.0 and above, we will use `spark-connect-repl` instead of `cs`.
