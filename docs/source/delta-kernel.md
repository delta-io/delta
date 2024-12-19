---
description: Learn how to build connectors to read and write Delta tables.
---

.. toctree::
    :maxdepth: 2
    :glob:

# Delta Kernel

The Delta Kernel project is a set of libraries ([Java](delta-kernel-java.md) and [Rust](delta-kernel-rust.md)) for building Delta connectors that can read from and write into Delta tables without the need to understand the [Delta protocol details](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).

You can use this library to do the following:
- Read data from small Delta tables in a single thread in a single process.
- Read data from large Delta tables using multiple threads in a single process.
- Build a complex connector for a distributed processing engine and read very large Delta tables.
- Insert data into a Delta table either from a single process or a complex distributed engine.

Here is an example of a simple table scan with a filter:

```java
Engine myEngine = DefaultEngine.create() ;                  // define an engine (more details below)
Table myTable = Table.forPath("/delta/table/path");         // define what table to scan
Snapshot mySnapshot = myTable.getLatestSnapshot(myEngine);  // define which version of table to scan
Scan myScan = mySnapshot.getScanBuilder(myEngine)           // specify the scan details
  .withFilters(myEngine, scanFilter)
  .build();
CloseableIterator<ColumnarBatch> physicalData =             // read the Parquet data files
  .. read from Parquet data files ...
Scan.transformPhysicalData(...)                             // returns the table data
```

A complete version of the above example program and more examples of reading from and writing into a Delta table are available [here](https://github.com/delta-io/delta/tree/master/kernel/examples).

Notice that there are two sets of public APIs to build connectors.
- **Table APIs** - Interfaces like [`Table`](https://delta-io.github.io/delta/snapshot/kernel-api/java/index.html?io/delta/kernel/Table.html) and [`Snapshot`](https://delta-io.github.io/delta/snapshot/kernel-api/java/index.html?io/delta/kernel/Snapshot.html) that allow you to read (and soon write to) Delta tables
- **Engine APIs** - The [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java//index.html?io/delta/kernel/engine/Engine.html) interface allows you to plug in connector-specific optimizations to compute-intensive components in the Kernel. For example, Delta Kernel provides a *default* Parquet file reader via the `DefaultEngine`, but you may choose to replace that default with a custom `Engine` implementation that has a faster Parquet reader for your connector/processing engine.

## Kernel Java
The Java Kernel is a set of libraries for building Delta connectors in Java. See [Kernel Java](delta-kernel-java.md) for more information.

## Kernel Rust
The Rust Kernel is a set of libraries for building Delta connectors in native languages. See [Kernel Rust](delta-kernel-rust.md) for more information.


## More Information
- [Talk](https://www.youtube.com/watch?v=KVUMFv7470I) explaining the rationale behind Kernel and the API design (slides are available [here](https://docs.google.com/presentation/d/1PGSSuJ8ndghucSF9GpYgCi9oeRpWolFyehjQbPh92-U/edit) which are kept up-to-date with the changes).
- [User guide](https://github.com/delta-io/delta/blob/master/kernel/USER_GUIDE.md) on the step-by-step process of using Kernel in a standalone Java program or in a distributed processing connector for reading and writing to Delta tables.
- Example [Java programs](https://github.com/delta-io/delta/tree/master/kernel/examples) that illustrate how to read and write Delta tables using the Kernel APIs.
- Table and default Engine API Java [documentation](https://docs.delta.io/latest/api/java/kernel/index.html)
- [Migration guide](https://github.com/delta-io/delta/blob/master/kernel/USER_GUIDE.md#migration-guide)
