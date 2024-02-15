# Delta Kernel

The Delta Kernel project is a set of Java libraries for building Delta connectors that can read (and soon, write to) Delta tables without the need to understand the [Delta protocol details](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).

You can use this library to do the following:
- Read data from small Delta tables in a single thread in a single process.
- Read data from large Delta tables using multiple threads in a single process.
- Build a complex connector for a distributed processing engine and read very large Delta tables.
- [soon!] Write to Delta tables from multiple threads / processes / distributed engines.

Here is an example of a simple table scan with a filter:
```java
TableClient myTableClient = DefaultTableClient.create() ;        // define a client (more details below)
Table myTable = Table.forPath("/delta/table/path");              // define what table to scan
Snapshot mySnapshot = myTable.getLatestSnapshot(myTableClient);  // define which version of table to scan
Scan myScan = mySnapshot.getScanBuilder(myTableClient)           // specify the scan details
  .withFilters(myTableClient, scanFilter)
  .build();
CloseableIterator<ColumnarBatch> physicalData =                  // read the Parquet data files
  .. read from Parquet data files ...
Scan.transformPhysicalData(...)                                  // returns the table data
```

A complete version of the above example program is available [here](https://github.com/delta-io/delta/tree/master/kernel/examples).

Notice that there two sets of public APIs to build connectors. 
- **Table APIs** - Interfaces like [`Table`](https://delta-io.github.io/delta/snapshot/kernel-api/java/index.html?io/delta/kernel/Table.html) and [`Snapshot`](https://delta-io.github.io/delta/snapshot/kernel-api/java/index.html?io/delta/kernel/Snapshot.html) that allow you to read (and soon write to) Delta tables
- **TableClient APIs** - The [`TableClient`](https://delta-io.github.io/delta/snapshot/kernel-api/java//index.html?io/delta/kernel/client/TableClient.html) interface allow you to plug in connector-specific optimizations to compute intensive components in the Kernel. For example, Delta Kernel provides a *default* Parquet file reader via the `DefaultTableClient`, but you may choose to replace that default with a custom `TableClient` implementation that has a faster Parquet reader for your connector/processing engine.

# Project setup with Delta Kernel 
The Delta Kernel project provides the following two Maven artifacts:
- `delta-kernel-api`: This is a must-have dependency and contains all the public `Table` and `TableClient` APIs discussed earlier.
- `delta-kernel-defaults`: This is an optional dependency that contains *default* implementations of the `TableClient` interfaces using Hadoop libraries. Developers can optionally use these default implementations to speed up the development of their Delta connector.
```xml
<!-- Must have dependency -->
<dependency>
  <groupId>io.delta</groupId>
  <artifactId>delta-kernel-api</artifactId>
  <version>VERSION</version>
</dependency>

<!-- Optional dependency -->
<dependency>
  <groupId>io.delta</groupId>
  <artifactId>delta-kernel-defaults</artifactId>
  <version>VERSION</version>
</dependency>
```

# API Guarantees
**Note: This project is currently in `preview` and all APIs are currently in an evolving state. We welcome trying out the APIs to build Delta Lake connectors and  providing feedback (see below) to the project authors.**

The Java API docs are available [here](https://delta-io.github.io/delta/snapshot/kernel-api/java/index.html). Only the classes and interfaces documented here are considered as public APIs with backward compatibility guarantees (when marked as Stable APIs). All other classes and interfaces available in the JAR are considered as private APIs with no stability guarantees. 

Detailed user guide explaining the APIs and how to use them is available [here](https://github.com/delta-io/delta/blob/master/kernel/USER_GUIDE.md).

Example Java programs that read Delta Lake tables using the Kernel APIs are avilable [here](https://github.com/delta-io/delta/tree/master/kernel/examples).

# Providing feedback
We use [GitHub Issues](https://github.com/delta-io/delta/issues) to track community-reported issues. You can also contact the community to get answers.

# Contributing
We welcome contributions to Delta Lake and we accept contributions via Pull Requests. See our [CONTRIBUTING.md](https://github.com/delta-io/delta/blob/master/CONTRIBUTING.md) for more details. We also adhere to the [Delta Lake Code of Conduct](https://github.com/delta-io/delta/blob/master/CODE_OF_CONDUCT.md).
