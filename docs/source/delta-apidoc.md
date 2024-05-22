---
description: Learn about the APIs provided by <Delta>.
---

# <Delta> APIs

.. note:: Some <Delta> APIs are still evolving and are indicated with the **Evolving** qualifier or annotation in the API docs.

## Delta Spark

Delta Spark is a library for reading and writing Delta tables using Apache Spark™. For most read and write operations on Delta tables, you can use <AS> [reader and writer](/spark/latest/dataframes-datasets/index.md) APIs. For examples,  see [_](delta-batch.md) and [_](delta-streaming.md).

However, there are some operations that are specific to <Delta> and you must use <Delta> APIs. For examples, see [_](delta-utility.md).

- [Scala API docs](api/scala/spark/io/delta/tables/index.html)
- [Java API docs](api/java/spark/index.html)
- [Python API docs](api/python/spark/index.html)

## Delta Standalone
Delta Standalone, formerly known as the Delta Standalone Reader (DSR), is a JVM library to read and write Delta tables. Unlike Delta-Spark, this library doesn't use Spark to read or write tables and it has only a few transitive dependencies. It can be used by any application that cannot use a Spark cluster. More details refer [here](https://github.com/delta-io/delta/blob/master/connectors/README.md).

- [Java API docs](api/java/standalone/index.html)

## Delta Flink
Flink/Delta Connector is a JVM library to read and write data from Apache Flink applications to Delta tables utilizing the Delta Standalone JVM library. More details refer [here](https://github.com/delta-io/delta/blob/master/connectors/flink/README.md).

- [Java API docs](api/java/flink/index.html)

## Delta Kernel

Delta Kernel is a library for operating on Delta tables. Specifically, it provides simple and narrow APIs for reading and writing to Delta tables without the need to understand the [Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) details. You can use this library to do the following:
- Read Delta tables from your applications.
- Build a connector for a distributed engine like Apache Spark™, Apache Flink, or Trino for reading massive Delta tables.

More details refer [here](https://github.com/delta-io/delta/blob/branch-3.0/kernel/USER_GUIDE.md).

- [Java API docs](api/java/kernel/index.html)

.. include:: /shared/replacements.md
