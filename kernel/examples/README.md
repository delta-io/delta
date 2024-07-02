# Delta Kernel Examples

The Delta Kernel project is a set of Java libraries for building Delta connectors that can read from and write into Delta tables without the need to understand the [Delta protocol details](https://github.com/delta-io/delta/blob/master/PROTOCOL.md). Visit [Delta Kernel](../README.md) for details.

This directory contains a set of example programs that show how to read a Delta table and write data into it. The goals of these examples are:
* Connector/Delta Lake developers: to provide a starting point in using Kernel to read or write Delta tables.
* Delta Lake developers: to verify pre-release Kernel jar as part of the Delta release verification.

### Examples
* [Reading a Delta table serially from a single thread](https://github.com/delta-io/delta/blob/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples/SingleThreadedTableReader.java).
* [Reading a Delta table parellelly using multiple threads](https://github.com/delta-io/delta/blob/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples/MultiThreadedTableReader.java). This program illustrates the end-2-end usage of Delta Kernel APIs to read a table in multi-threaded and/or distributed environement. The steps are:
  * On driver (or co-ordinator - basically the node where the planning and query execution coordination happens) fetches the list of scan file to read from Delta Kernel.
  * Driver serializes the scan file infos (in the example to JSON) and sends them to workers (in the example worker is another thread, but it can be another node).
  * Workers deserializes the scan file infos and makes use of Kernel to read the data.
* [Creating Delta tables (either partitioned or un-partitioned)](https://github.com/delta-io/delta/blob/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples/CreateTable.java)
* [Creating and inserting data into Delta tables](https://github.com/delta-io/delta/blob/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples/CreateTableAndInsertData.java). Contains examples for inserting data into partitioned or un-partitioned tables.
* [Idempotently inserting data to a Delta table](https://github.com/delta-io/delta/blob/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples/CreateTableAndInsertData.java). Check `idempotentInserts` method.
* [Inserting and optionally checkpointing a Delta table](https://github.com/delta-io/delta/blob/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples/CreateTableAndInsertData.java). Check `insertWithOptionalCheckpoint` method.

### More information
- [User guide](https://github.com/delta-io/delta/blob/master/kernel/USER_GUIDE.md) on the step-by-step process of using Kernel in a standalone Java program or in a distributed processing connector for reading and writing to Delta tables.
- Table and default Engine API Java [documentation](https://docs.delta.io/latest/api/java/kernel/index.html)


### Running integration tests
To run examples by building the artifacts from code and using them:

```
<delta-repo-root>/kernel/examples/run-kernel-examples.py --use-local
```

To run examples using artifacts from a Maven repository:
```
<delta-repo-root>/kernel/examples/run-kernel-examples.py --version <version> --maven-repo <staged_repo_url>
```

