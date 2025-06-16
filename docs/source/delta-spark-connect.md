---
description: Learn about Delta Connect - Spark Connect Support in Delta.
---

# Delta Connect (aka Spark Connect Support in Delta)

.. note:: This feature is available in <Delta> 4.0.0 and above. Please note, Delta Connect is currently in preview and not recommended for production workloads.

Delta Connect adds [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) support to Delta Lake for Apache Spark. Spark Connect is a new initiative that adds a decoupled client-server infrastructure which allows remote connectivity from Spark from everywhere. Delta Connect allows all Delta Lake operations to work in your application running as a client connected to the Spark server.

## Motivation

Delta Connect is expected to bring the same benefits as Spark Connect:

1. Upgrading to more recent versions of Spark and <Delta> is now easier because the client interface is being completely decoupled from the server.
2. Simpler integration of Spark and <Delta> with developer tooling. IDEs no longer have to integrate with the full Spark and <Delta> implementation, and instead can integrate with a thin-client.
3. Support for languages other than Java/Scala and Python. Clients "merely" have to generate Protocol Buffers and therefore become simpler to implement.
4. Spark and <Delta> will become more stable, as user code is no longer running in the same JVM as Spark's driver.
5. Remote connectivity. Code can run anywhere now, as there is a gRPC layer between the user interface and the driver.

## How to start the Spark Server with Delta

1. Download `spark-4.0.0-bin-hadoop3.tgz` from [Spark 4.0.0](https://archive.apache.org/dist/spark/spark-4.0.0).

2. Start the Spark Connect server with the <Delta> Connect plugins:

```bash
sbin/start-connect-server.sh \ 
  --packages io.delta:delta-connect-server_2.13:4.0.0,com.google.protobuf:protobuf-java:3.25.1 \ 
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.connect.extensions.relation.classes=org.apache.spark.sql.connect.delta.DeltaRelationPlugin" \
  --conf "spark.connect.extensions.command.classes=org.apache.spark.sql.connect.delta.DeltaCommandPlugin"
```

## How to use the Python Spark Connect Client with Delta

The <Delta> Connect Python client is included in the same PyPi package as <Delta> Spark.

1. `pip install pyspark==4.0.0`.

2. `pip install delta-spark==4.0.0`.

3. The usage is the same as Spark Connect (e.g. `./bin/pyspark --remote "sc://localhost"`).
We just need to pass in a remote `SparkSession` (instead of a local one) to the `DeltaTable` API.

An example:

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

deltaTable = DeltaTable.forName(spark, "my_table")
deltaTable.toDF().show()

deltaTable.update(
  condition = "id % 2 == 0",
  set = {"id": "id + 100"}
)
```

## How to use the Scala Spark Connect Client with Delta

Make sure you are using Java 17!

```bash
./bin/spark-shell --remote "sc://localhost" --packages io.delta:delta-connect-client_2.13:4.0.0,com.google.protobuf:protobuf-java:3.25.1
```

An example:
    
```scala
import io.delta.tables.DeltaTable

val deltaTable = DeltaTable.forName(spark, "my_table")
deltaTable.toDF.show()

deltaTable.updateExpr(
  condition = "id % 2 == 0",
  set = Map("id" -> "id + 100")
)
```

.. include:: /shared/replacements.md
