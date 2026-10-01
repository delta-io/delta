# Delta Kernel

The Delta Kernel project is a set of Java libraries for building Delta connectors that can read from and write into Delta tables without the need to understand the [Delta protocol details](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).

You can use this library to do the following:
- Read data from small Delta tables in a single thread in a single process.
- Read data from large Delta tables using multiple threads in a single process.
- Build a complex connector for a distributed processing engine and read very large Delta tables.
- Insert data into a Delta table either from a single process or a complex distributed engine.

Here is an example of a simple table scan with a filter:
```java
Engine myEngine = DefaultEngine.create() ;                  // define a engine (more details below)
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
- **Table APIs** - Interfaces like [`Table`](https://delta-io.github.io/delta/snapshot/kernel-api/java/index.html?io/delta/kernel/Table.html), [`Snapshot`](https://delta-io.github.io/delta/snapshot/kernel-api/java/index.html?io/delta/kernel/Snapshot.html), and [`Transaction`](https://delta-io.github.io/delta/snapshot/kernel-api/java/index.html?io/delta/kernel/Transaction.html) that allow you to read from and write to Delta tables
- **Engine APIs** - The [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java//index.html?io/delta/kernel/engine/Engine.html) interface allows you to plug in connector-specific optimizations to compute-intensive components in the Kernel. For example, Delta Kernel provides a *default* Parquet file reader via the `DefaultEngine`, but you may choose to replace that default with a custom `Engine` implementation that has a faster Parquet reader for your connector/processing engine.

# Project setup with Delta Kernel 
The Delta Kernel project provides the following two Maven artifacts:
- `delta-kernel-api`: This is a must-have dependency and contains all the public `Table` and `Engine` APIs discussed earlier.
- `delta-kernel-defaults`: This is an optional dependency that contains *default* implementations of the `Engine` interfaces using Hadoop libraries. Developers can optionally use these default implementations to speed up the development of their Delta connector.
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

The Java API docs are available [here](https://docs.delta.io/latest/api/java/kernel/index.html). Only the classes and interfaces documented here are considered public APIs with backward compatibility guarantees (when marked as **Stable** APIs). All other classes and interfaces available in the JAR are considered private APIs with no backward compatibility guarantees.

Kernel APIs are still evolving and new features are being added with every release. Kernel authors try to make the API changes backward compatible as much as they can with each new release even when an API is marked as **Evolving**, but sometimes it is hard to maintain the backward compatibility for a project that is evolving rapidly. With each new release, the [examples](https://github.com/delta-io/delta/tree/master/kernel/examples) are kept up-to-date with the latest API changes, and a detailed [migration guide](https://github.com/delta-io/delta/blob/master/kernel/USER_GUIDE.md#migration-guide) is provided for the connectors to upgrade to use the updated APIs.


# More Information
- [Talk](https://www.youtube.com/watch?v=KVUMFv7470I) explaining the rationale behind Kernel and the API design (slides are available [here](https://docs.google.com/presentation/d/1PGSSuJ8ndghucSF9GpYgCi9oeRpWolFyehjQbPh92-U/edit) which are kept up-to-date with the changes).
- [User guide](https://github.com/delta-io/delta/blob/master/kernel/USER_GUIDE.md) on the step-by-step process of using Kernel in a standalone Java program or in a distributed processing connector for reading and writing to Delta tables.
- Example [Java programs](https://github.com/delta-io/delta/tree/master/kernel/examples) that illustrate how to read and write Delta tables using the Kernel APIs.
- Table and default Engine API Java [documentation](https://docs.delta.io/latest/api/java/kernel/index.html)
- [Migration guide](https://github.com/delta-io/delta/blob/master/kernel/USER_GUIDE.md#migration-guide)


# Providing feedback
We use [GitHub Issues](https://github.com/delta-io/delta/issues) to track community-reported issues. You can also contact the community to get answers.

# Contributing
We welcome contributions to Delta Lake and we accept contributions via Pull Requests. See our [CONTRIBUTING.md](https://github.com/delta-io/delta/blob/master/CONTRIBUTING.md) for more details. We also adhere to the [Delta Lake Code of Conduct](https://github.com/delta-io/delta/blob/master/CODE_OF_CONDUCT.md).

# Setting up IDE

Java code adheres to the [Google style](https://google.github.io/styleguide/javaguide.html), which is verified via `build/sbt javafmtCheckAll` during builds.
In order to automatically fix Java code style issues, please use `build/sbt javafmtAll`.

## Configuring Code Formatter for Eclipse/IntelliJ

Follow the instructions for [Eclipse](https://github.com/google/google-java-format#eclipse) or
[IntelliJ](https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides) to install the **google-java-format** plugin (note the required manual actions for IntelliJ).
