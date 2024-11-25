## Delta Kernel User Guide

## What is Delta Kernel?
Delta Kernel is a library for operating on Delta tables. Specifically, it provides simple and narrow APIs for reading and writing to Delta tables without the need to understand the [Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) details. You can use this library to do the following:

* Read and write Delta tables from your applications.
* Build a connector for a distributed engine like [Apache Spark™](https://github.com/apache/spark), [Apache Flink](https://github.com/apache/flink), or [Trino](https://github.com/trinodb/trino) for reading or writing massive Delta tables.

   * [Delta Kernel User Guide](#delta-kernel-user-guide)
   * [What is Delta Kernel?](#what-is-delta-kernel)
   * [Set up Delta Kernel for your project](#set-up-delta-kernel-for-your-project)
   * [Read a Delta table in a single process](#read-a-delta-table-in-a-single-process)
      * [Step 1: Full scan on a Delta table](#step-1-full-scan-on-a-delta-table)
      * [Step 2: Improve scan performance with file skipping](#step-2-improve-scan-performance-with-file-skipping)
   * [Create a Delta table](#create-a-delta-table)
   * [Create a table and insert data into it](#create-a-table-and-insert-data-into-it)
   * [Blind append into an existing Delta table](#blind-append-into-an-existing-delta-table)
   * [Idempotent Blind Appends to a Delta Table](#idempotent-blind-appends-to-a-delta-table)
   * [Checkpointing a Delta table](#checkpointing-a-delta-table)
   * [Build a Delta connector for a distributed processing engine](#build-a-delta-connector-for-a-distributed-processing-engine)
      * [Step 0: Validate the prerequisites](#step-0-validate-the-prerequisites)
      * [Step 1: Set up Delta Kernel in your connector project](#step-1-set-up-delta-kernel-in-your-connector-project)
         * [Set up Java projects](#set-up-java-projects)
      * [Step 2: Build your own Engine](#step-2-build-your-own-engine)
         * [Step 2.1: Implement the Engine interface](#step-21-implement-the-engine-interface)
         * [Step 2.2: Implement FileSystemClient interface](#step-22-implement-filesystemclient-interface)
         * [Step 2.3: Implement ParquetHandler](#step-23-implement-parquethandler)
         * [Step 2.5: Implement JsonHandler](#step-25-implement-jsonhandler)
         * [Step 2.6: Implement ColumnarBatch and ColumnVector](#step-26-implement-columnarbatch-and-columnvector)
      * [Step 3: Build read support in your connector](#step-3-build-read-support-in-your-connector)
         * [Step 3.1: Resolve the table snapshot to query](#step-31-resolve-the-table-snapshot-to-query)
         * [Step 3.2: Resolve files to scan](#step-32-resolve-files-to-scan)
         * [Step 3.3: Distribute the file information to the workers](#step-33-distribute-the-file-information-to-the-workers)
         * [Step 3.4: Read the columnar data](#step-34-read-the-columnar-data)
      * [Step 4: Build write support in your connector](#step-4-build-write-support-in-your-connector)
         * [Step 4.1: Determine the schema of the data that needs to be written to the table](#step-41-determine-the-schema-of-the-data-that-needs-to-be-written-to-the-table)
         * [Step 4.2: Determine the physical partitioning of the data based on the table schema and partition columns](#step-42-determine-the-physical-partitioning-of-the-data-based-on-the-table-schema-and-partition-columns)
         * [Step 4.3: Distribute the writer tasks definitions (which include the transaction state) to workers](#step-43-distribute-the-writer-tasks-definitions-which-include-the-transaction-state-to-workers)
         * [Step 4.4: Tasks write the data to data files and send the data file info to the driver.](#step-44-tasks-write-the-data-to-data-files-and-send-the-data-file-info-to-the-driver)
         * [Step 4.5: Finalize the query.](#step-45-finalize-the-query)
   * [Migration guide](#migration-guide)
      * [Migration from Delta Lake version 3.1.0 to 3.2.0](#migration-from-delta-lake-version-310-to-320)

## Set up Delta Kernel for your project
You need to `io.delta:delta-kernel-api` and `io.delta:delta-kernel-defaults` dependencies. Following is an example Maven `pom` file dependency list.

The `delta-kernel-api` module contains the core of the Kernel that abstracts out the Delta protocol to enable reading and writing into Delta tables. It makes use of the `Engine` interface that is being passed to the Kernel API by the connector for heavy-lift operations such as reading/writing Parquet or JSON files, evaluating expressions or file system operations such as listing contents of the Delta Log directory, etc. Kernel supplies a default implementation of `Engine` in module `delta-kernel-defaults`. The connectors can implement their own version of `Engine` to make use of their native implementation of functionalities the `Engine` provides. For example: the connector can make use of their Parquet reader instead of using the reader from the `DefaultEngine`. More details on this [later](#step-2-build-your-own-engine).

```xml
<dependencies>
  <dependency>
    <groupId>io.delta</groupId>
    <artifactId>delta-kernel-api</artifactId>
    <version>${delta-kernel.version}</version>
  </dependency>

  <dependency>
    <groupId>io.delta</groupId>
    <artifactId>delta-kernel-defaults</artifactId>
    <version>${delta-kernel.version}</version>
  </dependency>
</dependencies>
```

If your connector is not using the [`DefaultEngine`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/Engine.html) provided by the Kernel, the dependency `delta-kernel-defaults` from the above list can be skipped.


## Read a Delta table in a single process
In this section, we will walk through how to build a very simple single-process Delta connector that can read a Delta table using the default [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/Engine.html) implementation provided by Delta Kernel.

You can either write this code yourself in your project, or you can use the [examples](https://github.com/delta-io/delta/tree/master/kernel/examples) present in the Delta code repository.

### Step 1: Full scan on a Delta table
The main entry point is [`io.delta.kernel.Table`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/Table.html) which is a programmatic representation of a Delta table. Say you have a Delta table at the directory `myTablePath`. You can create a `Table` object as follows:

```java
import io.delta.kernel.*;
import io.delta.kernel.defaults.*;
import org.apache.hadoop.conf.Configuration;

String myTablePath = <my-table-path>; // fully qualified table path. Ex: file:/user/tables/myTable
Configuration hadoopConf = new Configuration();
Engine myEngine = DefaultEngine.create(hadoopConf);
Table myTable = Table.forPath(myEngine, myTablePath);
```

Note the default [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/Engine.html) we are creating to bootstrap the `myTable` object. This object allows you to plug in your own libraries for computationally intensive operations like Parquet file reading, JSON parsing, etc. You can ignore it for now. We will discuss more about this later when we discuss how to build more complex connectors for distributed processing engines.

From this `myTable` object you can create a [`Snapshot`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/Snapshot.html) object which represents the consistent state (a.k.a. a snapshot consistency) in a specific version of the table.  

```java
Snapshot mySnapshot = myTable.getLatestSnapshot(myEngine);
```

Now that we have a consistent snapshot view of the table, we can query more details about the table. For example, you can get the version and schema of this snapshot.

```java
long version = mySnapshot.getVersion(myEngine);
StructType tableSchema = mySnapshot.getSchema(myEngine);
```

Next, to read the table data, we have to *build* a [`Scan`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/Scan.html) object. In order to build a `Scan` object, create a [`ScanBuilder`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/ScanBuilder.html) object which optionally allows selecting a subset of columns to read or setting a query filter. For now, ignore these optional settings.

```java
Scan myScan = mySnapshot.getScanBuilder(myEngine).build()

// Common information about scanning for all data files to read.
Row scanState = myScan.getScanState(myEngine)

// Information about the list of scan files to read
CloseableIterator<FilteredColumnarBatch> scanFiles = myScan.getScanFiles(myEngine)
```

This [`Scan`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/Scan.html) object has all the necessary metadata to start reading the table. There are two crucial pieces of information needed for reading data from a file in the table. 

* `myScan.getScanFiles(Engine)`:  Returns scan files as columnar batches (represented as an iterator of `FilteredColumnarBatch`es, more on that later) where each selected row in the batch has information about a single file containing the table data.
* `myScan.getScanState(Engine)`: Returns the snapshot-level information needed for reading any file. Note that this is a single row and common to all scan files.

For each scan file the physical data must be read from the file. The columns to read are specified in the scan file state. Once the physical data is read, you have to call [`ScanFile.transformPhysicalData(...)`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/Scan.html#transformPhysicalData-io.delta.kernel.engine.Engine-io.delta.kernel.data.Row-io.delta.kernel.data.Row-io.delta.kernel.utils.CloseableIterator-) with the scan state and the physical data read from scan file. This API takes care of transforming (e.g. adding partition columns) the physical data into logical data of the table. Here is an example of reading all the table data in a single thread.

```java
CloserableIterator<FilteredColumnarBatch> fileIter = scanObject.getScanFiles(myEngine);

Row scanStateRow = scanObject.getScanState(myEngine);

while(fileIter.hasNext()) {
  FilteredColumnarBatch scanFileColumnarBatch = fileIter.next();

  // Get the physical read schema of columns to read from the Parquet data files
  StructType physicalReadSchema =
    ScanStateRow.getPhysicalDataReadSchema(engine, scanStateRow);

  try (CloseableIterator<Row> scanFileRows = scanFileColumnarBatch.getRows()) {
    while (scanFileRows.hasNext()) {
      Row scanFileRow = scanFileRows.next();

      // From the scan file row, extract the file path, size and modification time metadata
      // needed to read the file.
      FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);

      // Open the scan file which is a Parquet file using connector's own
      // Parquet reader or default Parquet reader provided by the Kernel (which
      // is used in this example).
      CloseableIterator<ColumnarBatch> physicalDataIter =
        engine.getParquetHandler().readParquetFiles(
          singletonCloseableIterator(fileStatus),
          physicalReadSchema,
          Optional.empty() /* optional predicate the connector can apply to filter data from the reader */
        );

      // Now the physical data read from the Parquet data file is converted to a table
      // logical data. Logical data may include the addition of partition columns and/or
      // subset of rows deleted
      try (
         CloseableIterator<FilteredColumnarBatch> transformedData =
           Scan.transformPhysicalData(
             engine,
             scanStateRow,
             scanFileRow,
             physicalDataIter)) {
        while (transformedData.hasNext()) {
          FilteredColumnarBatch logicalData = transformedData.next();
          ColumnarBatch dataBatch = logicalData.getData();

          // Not all rows in `dataBatch` are in the selected output.
          // An optional selection vector determines whether a row with a
          // specific row index is in the final output or not.
          Optional<ColumnVector> selectionVector = dataReadResult.getSelectionVector();

          // access the data for the column at ordinal 0
          ColumnVector column0 = dataBatch.getColumnVector(0);
          for (int rowIndex = 0; rowIndex < column0.getSize(); rowIndex++) {
            // check if the row is selected or not
            if (!selectionVector.isPresent() || // there is no selection vector, all records are selected
               (!selectionVector.get().isNullAt(rowId) && selectionVector.get().getBoolean(rowId)))  {
              // Assuming the column type is String.
              // If it is a different type, call the relevant function on the `ColumnVector`
              System.out.println(column0.getString(rowIndex));
            }
          }

	  // access the data for column at ordinal 1
	  ColumnVector column1 = dataBatch.getColumnVector(1);
	  for (int rowIndex = 0; rowIndex < column1.getSize(); rowIndex++) {
            // check if the row is selected or not
            if (!selectionVector.isPresent() || // there is no selection vector, all records are selected
               (!selectionVector.get().isNullAt(rowId) && selectionVector.get().getBoolean(rowId)))  {
              // Assuming the column type is Long.
              // If it is a different type, call the relevant function on the `ColumnVector`
              System.out.println(column1.getLong(rowIndex));
            }
          }
	  // .. more ..
        }
      }
    }
  }
}
```

A few working examples to read Delta tables within a single process are available [here](https://github.com/delta-io/delta/tree/master/kernel/examples).

> [!IMPORTANT]
> All the Delta protocol-level details are encoded in the rows returned by `Scan.getScanFiles` API, but you do not have to understand them in order to read the table data correctly. All you need is to get the Parquet file status from each scan file row and read the data from the Parquet file into the `ColumnarBatch` format. The physical data is converted into the logical data of the table using [`Scan.transformPhysicalData`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/Scan.html#transformPhysicalData-io.delta.kernel.engine.Engine-io.delta.kernel.data.Row-io.delta.kernel.data.Row-io.delta.kernel.utils.CloseableIterator-). Transformation to logical data is dictated by the protocol and the metadata of the table and the scan file. As the Delta protocol evolves this transformation step will evolve with it and your code will not have to change to accommodate protocol changes. This is the major advantage of the abstractions provided by Delta Kernel.

> [!NOTE]
> Observe that the same `Engine` instance `myEngine` is passed multiple times whenever a call to Delta Kernel API is made. The reason for passing this instance for every call is because it is the connector context, it should maintained outside of the Delta Kernel APIs to give the connector control over the `Engine`.

### Step 2: Improve scan performance with file skipping
We have explored how to do a full table scan. However, the real advantage of using the Delta format is that you can skip files using your query filters. To make this possible, Delta Kernel provides an [expression framework](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/expressions/package-summary.html) to encode your filters and provide them to Delta Kernel to skip files during the scan file generation. For example, say your table is partitioned by `columnX`, you want to query only the partition `columnX=1`. You can generate the expression and use it to build the scan as follows:

```java
import io.delta.kernel.expressions.*;

import io.delta.kernel.defaults.engine.*;

Engine myEngine = DefaultEngine.create(new Configuration());

Predicate filter = new Predicate(
  "=",
  Arrays.asList(new Column("columnX"), Literal.ofInt(1)));

Scan myFilteredScan = mySnapshot.buildScan(engine)
  .withFilter(myEngine, filter)
  .build()

// Subset of the given filter that is not guaranteed to be satisfied by
// Delta Kernel when it returns data. This filter is used by Delta Kernel
// to do data skipping as much as possible. The connector should use this filter
// on top of the data returned by Delta Kernel in order for further filtering.
Optional<Predicate> remainingFilter = myFilteredScan.getRemainingFilter();
```

The scan files returned by  `myFilteredScan.getScanFiles(myEngine)` will have rows representing files only of the required partition. Similarly, you can provide filters for non-partition columns, and if the data in the table is well clustered by those columns, then Delta Kernel will be able to skip files as much as possible.

## Create a Delta table
In this section, we will walk through how to build a Delta connector that can create a Delta table using the default [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/Engine.html) implementation provided by Delta Kernel.

You can either write this code yourself in your project, or you can use the [examples](https://github.com/delta-io/delta/tree/master/kernel/examples) present in the Delta code repository.

The main entry point is [`io.delta.kernel.Table`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/Table.html) which is a programmatic representation of a Delta table. Say you want to create Delta table at the directory `myTablePath`. You can create a `Table` object as follows:

```java
package io.delta.kernel.examples;

import io.delta.kernel.*;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterable;

String myTablePath = <my-table-path>; 
Configuration hadoopConf = new Configuration();
Engine myEngine = DefaultEngine.create(hadoopConf);
Table myTable = Table.forPath(myEngine, myTablePath);
```

Note the default [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/Engine.html) we are creating to bootstrap the `myTable` object. This object allows you to plug in your own libraries for computationally intensive operations like Parquet file reading, JSON parsing, etc. You can ignore it for now. We will discuss more about this later when we discuss how to build more complex connectors for distributed processing engines.

From this `myTable` object you can create a [`TransactionBuilder`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/TransactionBuilder.html) object which allows you to construct a [`Transaction`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/Transaction.html) object

```java
TransactionBuilder txnBuilder =
  myTable.createTransactionBuilder(
    myEngine,
    "Examples", /* engineInfo - connector can add its own identifier which is noted in the Delta Log */ 
    Operation.CREATE_TABLE /* What is the operation we are trying to perform. This is noted in the Delta Log */
  );
```

Now that you have the `TransactionBuilder` object, you can set the table schema and partition columns of the table.

```java
StructType mySchema = new StructType()
  .add("id", IntegerType.INTEGER)
  .add("name", StringType.STRING)
  .add("city", StringType.STRING)
  .add("salary", DoubleType.DOUBLE);

// Partition columns are optional. Use it only if you are creating a partitioned table.
List<String> myPartitionColumns = Collections.singletonList("city");

// Set the schema of the new table on the transaction builder
txnBuilder = txnBuilder
  .withSchema(engine, mySchema);

// Set the partition columns of the new table only if you are creating
// a partitioned table; otherwise, this step can be skipped.
txnBuilder = txnBuilder
  .withPartitionColumns(engine, examplePartitionColumns);
```

`TransactionBuilder` allows setting additional properties of the table such as enabling a certain Delta feature or setting identifiers for idempotent writes. We will be visiting these in the next sections. The next step is to build `Transaction` out of the `TransactionBuilder` object.


```java
// Build the transaction
Transaction txn = txnBuilder.build(engine);
```

`Transaction` object allows the connector to optionally add any data and finally commit the transaction. A successful commit ensures that the table is created with the given schema. In this example, we are just creating a table and not adding any data as part of the table.

```java
// Commit the transaction.
// As we are just creating the table and not adding any data, the `dataActions` is empty.
TransactionCommitResult commitResult =
  txn.commit(
    engine,
    CloseableIterable.emptyIterable() /* dataActions */
  );
```

The [`TransactionCommitResult`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/TransactionCommitResult.html) contains the what version the transaction is committed as and whether the table is ready for a checkpoint. As we are creating a table the version will be `0`. We will be discussing later on what a checkpoint is and what it means for the table to be ready for the checkpoint.

A few working examples to create partitioned and un-partitioned Delta tables are available [here](https://github.com/delta-io/delta/tree/master/kernel/examples).

## Create a table and insert data into it
In this section, we will walk through how to build a Delta connector that can create a Delta table and insert data into the table (similar to `CREATE TABLE <table> AS <query>` construct in SQL) using the default [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/Engine.html) implementation provided by Delta Kernel.

You can either write this code yourself in your project, or you can use the [examples](https://github.com/delta-io/delta/tree/master/kernel/examples) present in the Delta code repository.

The first step is to construct a `Transaction`. Below is the code for that. For more details on what each step of the code means, please read the [create table](#create-a-delta-table) section.

```
package io.delta.kernel.examples;

import io.delta.kernel.*;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterable;

String myTablePath = <my-table-path>; 
Configuration hadoopConf = new Configuration();
Engine myEngine = DefaultEngine.create(hadoopConf);
Table myTable = Table.forPath(myEngine, myTablePath);

StructType mySchema = new StructType()
  .add("id", IntegerType.INTEGER)
  .add("name", StringType.STRING)
  .add("city", StringType.STRING)
  .add("salary", DoubleType.DOUBLE);

// Partition columns are optional. Use it only if you are creating a partitioned table.
List<String> myPartitionColumns = Collections.singletonList("city");

TransactionBuilder txnBuilder =
  myTable.createTransactionBuilder(
    myEngine,
    "Examples", /* engineInfo - connector can add its own identifier which is noted in the Delta Log */ 
    Operation.WRITE /* What is the operation we are trying to perform? This is noted in the Delta Log */
  );

// Set the schema of the new table on the transaction builder
txnBuilder = txnBuilder
  .withSchema(engine, mySchema);

// Set the partition columns of the new table only if you are creating
// a partitioned table; otherwise, this step can be skipped.
txnBuilder = txnBuilder
  .withPartitionColumns(engine, examplePartitionColumns);

// Build the transaction
Transaction txn = txnBuilder.build(engine);
```

Now that we have the [`Transaction`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/Transaction.html) object, the next step is generating the data that confirms the table schema and partitioned according to the table partitions.

```java
StructType dataSchema = txn.getSchema(engine)

// Optional for un-partitioned tables
List<String> partitionColumnNames = txn.getPartitionColumns(engine)
```

Using the data schema and partition column names the connector can plan the query and generate data. At tasks that actually have the data to write to the table, the connector can ask the Kernel to transform the data given in the table schema into physical data that can actually be written to the Parquet data files. For partitioned tables, the data needs to be first partitioned by the partition columns, and then the connector should ask the Kernel to transform the data for each partition separately. The partitioning step is needed because any given data file in the Delta table contains data belonging to exactly one partition.

Get the state of the transaction. The transaction state contains the information about how to convert the data in the table schema into physical data that needs to be written. The transformations depend on the protocol and features the table has.

```java
Row txnState = txn.getTransactionState(engine);
```

Prepare the data.

```java
// The data generated by the connector to write into a table
CloseableIterator<FilteredColumnarBatch> data = ... 

// Create partition value map
Map<String, Literal> partitionValues =
  Collections.singletonMap(
    "city", // partition column name
     // partition value. Depending upon the partition column type, the
     // partition value should be created. In this example, the partition
     // column is of type StringType, so we are creating a string literal.
     Literal.ofString(city)
  );
```

The connector data is passed as an iterator of [`FilteredColumnarBatch`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/FilteredColumnarBatch.html). Each of the `FilteredColumnarBatch` contains a [`ColumnarBatch`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/ColumnarBatch.html) which actually contains the data in columnar access format and an optional section vector that allows the connector to specify which rows from the `ColumnarBatch` to write to the table.

Partition values are passed as a map of the partition column name to the partition value. For an un-partitioned table, the map should be empty as it has no partition columns.

```
// Transform the logical data to physical data that needs to be written to the Parquet
// files
CloseableIterator<FilteredColumnarBatch> physicalData =
  Transaction.transformLogicalData(engine, txnState, data, partitionValues);
```

The above code converts the given data for partitions into an iterator of `FilteredColumnarBatch` that needs to be written to the Parquet data files. In order to write the data files, the connector needs to get the [`WriteContext`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/DataWriteContext.html) from Kernel, which tells the connector where to write the data files and what columns to collect statistics from each data file.

```java
// Get the write context
DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, partitionValues);
```

Now, the connector has the physical data that needs to be written to Parquet data files, and where those files should be written, it can start writing the data files.

```java
CloseableIterator<DataFileStatus> dataFiles = engine.getParquetHandler()
  .writeParquetFiles(
    writeContext.getTargetDirectory(),
    physicalData,
    writeContext.getStatisticsColumns()
  );
```

In the above code, the connector is making use of the `Engine` provided `ParquetHandler` to write the data, but the connector can choose its own Parquet file writer to write the data. Also note that the return of the above call is an iterator of [`DataFileStatus`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/utils/DataFileStatus.html) for each data file written. It basically contains the file path, file metadata, and optional file-level statistics for columns specified by the [`WriteContext.getStatisticsColumns()`]([https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/DataWriteContext.html](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/DataWriteContext.html#getStatisticsColumns--))

Convert each `DataFileStatus` into a Delta log action that can be written to the Delta table log.

```java
CloseableIterator<Row> dataActions =
  Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);
```

The next step is constructing [`CloseableIterable`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/utils/CloseableIterable.html) out of the all the Delta log actions generated above. The reason for constructing an `Iterable` is that the transaction committing involves accessing the list of Delta log actions more than one time (in order to resolve conflicts when there are multiple writes to the table). Kernel provides a [utility method](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/utils/CloseableIterable.html#inMemoryIterable-io.delta.kernel.utils.CloseableIterator-) to create an in-memory version of `CloseableIterable`. This interface also gives the connector an option to implement a custom implementation that spills the data actions to disk when the contents are too big to fit in memory.

```java
// Create a iterable out of the data actions. If the contents are too big to fit in memory,
// the connector may choose to write the data actions to a temporary file and return an
// iterator that reads from the file.
CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(dataActions);
```

The final step is committing the transaction!

```java
TransactionCommitStatus commitStatus = txn.commit(engine, dataActionsIterable)
```

The [`TransactionCommitResult`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/TransactionCommitResult.html) contains the what version the transaction is committed as and whether the table is ready for a checkpoint. As we are creating a table the version will be `0`. We will be discussing later on what a checkpoint is and what it means for the table to be ready for the checkpoint.

A few working examples to create and insert data into partitioned and un-partitioned Delta tables are available [here](https://github.com/delta-io/delta/tree/master/kernel/examples).

## Blind append into an existing Delta table
In this section, we will walk through how to build a Delta connector that inserts data into an existing Delta table (similar to `INSERT INTO <table> <query>` construct in SQL) using the default [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/Engine.html) implementation provided by Delta Kernel.

You can either write this code yourself in your project, or you can use the [examples](https://github.com/delta-io/delta/tree/master/kernel/examples) present in the Delta code repository. The steps are exactly similar to [Create table and insert data into it](#create-a-table-and-insert-data-into-it) except that we won't be providing any schema or partition columns when building the `TransactionBuilder`

```java
// Create a `Table` object with the given destination table path
Table table = Table.forPath(engine, tablePath);

// Create a transaction builder to build the transaction
TransactionBuilder txnBuilder =
  table.createTransactionBuilder(
    engine,
    "Examples", /* engineInfo */
    Operation.WRITE
  );

/ Build the transaction - no need to provide the schema as the table already exists.
Transaction txn = txnBuilder.build(engine);

// Get the transaction state
Row txnState = txn.getTransactionState(engine);

List<Row> dataActions = new ArrayList<>();

// Generate the sample data for three partitions. Process each partition separately.
// This is just an example. In a real-world scenario, the data may come from different
// partitions. Connectors already have the capability to partition by partition values
// before writing to the table

// In the test data `city` is a partition column
for (String city : Arrays.asList("San Francisco", "Campbell", "San Jose")) {
  FilteredColumnarBatch batch1 = generatedPartitionedDataBatch(
	    5 /* offset */, city /* partition value */);
  FilteredColumnarBatch batch2 = generatedPartitionedDataBatch(
	    5 /* offset */, city /* partition value */);
  FilteredColumnarBatch batch3 = generatedPartitionedDataBatch(
	    10 /* offset */, city /* partition value */);

    CloseableIterator<FilteredColumnarBatch> data =
	    toCloseableIterator(Arrays.asList(batch1, batch2, batch3).iterator());

    // Create partition value map
    Map<String, Literal> partitionValues =
	    Collections.singletonMap(
		    "city", // partition column name
		    // partition value. Depending upon the parition column type, the
		    // partition value should be created. In this example, the partition
		    // column is of type StringType, so we are creating a string literal.
		    Literal.ofString(city));


    // First transform the logical data to physical data that needs to be written
    // to the Parquet
    // files
    CloseableIterator<FilteredColumnarBatch> physicalData =
	    Transaction.transformLogicalData(engine, txnState, data, partitionValues);

    // Get the write context
    DataWriteContext writeContext =
	    Transaction.getWriteContext(engine, txnState, partitionValues);


    // Now write the physical data to Parquet files
    CloseableIterator<DataFileStatus> dataFiles = engine.getParquetHandler()
	    .writeParquetFiles(
		    writeContext.getTargetDirectory(),
		    physicalData,
		    writeContext.getStatisticsColumns());


    // Now convert the data file status to data actions that needs to be written to the Delta
    // table log
    CloseableIterator<Row> partitionDataActions = Transaction.generateAppendActions(
	    engine,
	    txnState,
	    dataFiles,
	    writeContext);

    // Now add all the partition data actions to the main data actions list. In a
    // distributed query engine, the partition data is written to files at tasks on executor
    // nodes. The data actions are collected at the driver node and then written to the
    // Delta table log using the `Transaction.commit`
    while (partitionDataActions.hasNext()) {
	dataActions.add(partitionDataActions.next());
    }
}

// Create a iterable out of the data actions. If the contents are too big to fit in memory,
// the connector may choose to write the data actions to a temporary file and return an
// iterator that reads from the file.
CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(
	toCloseableIterator(dataActions.iterator()));

// Commit the transaction.
TransactionCommitResult commitResult = txn.commit(engine, dataActionsIterable);
```

## Idempotent Blind Appends to a Delta Table
Idempotent writes allow the connector to make sure the data belonging to a particular transaction version and application id is inserted into the table at most once. In incremental processing systems (e.g. streaming systems), track progress using their own application-specific versions need to record what progress has been made in order to avoid duplicating data in the face of failures and retries during writes. By setting the transaction identifier, the Delta table can ensure that the data with the same identifier is not written multiple times. For more information refer to the Delta protocol section [Transaction Identifiers](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#transaction-identifiers)

To make the data append idempotent, set the transaction identifier on the [`TransactionBuilder`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/TransactionBuilder.html#withTransactionId-io.delta.kernel.engine.Engine-java.lang.String-long-)

```java
// Set the transaction identifiers for idempotent writes
// Delta/Kernel makes sure that there exists only one transaction in the Delta log
// with the given application id and txn version
txnBuilder =
  txnBuilder.withTransactionId(
    engine,
    "my app id", /* application id */
    100 /* monotonically increasing txn version with each new data insert */
  );
```

That's all the connector need to do for idempotent blind appends.

## Checkpointing a Delta table
[Checkpoints](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoints) are an optimization in Delta Log in order to construct the state of the Delta table faster. It basically contains the state of the table at the version the checkpoint is created. Delta Kernel allows the connector to optionally make the checkpoints. It is created for every few commits (configurable table property) on the table.

The result of `Transaction.commit` returns a `TransactionCommitResult` that contains the version the transaction is committed as and whether the table is [read for checkpoint](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/TransactionCommitResult.html#isReadyForCheckpoint--). Creating a checkpoint takes time as it needs to construct the entire state of the table. If the connector doesn't want to checkpoint by itself but uses other connectors that are faster in creating a checkpoint, it can skip the checkpointing step. 

If it wants to checkpoint, the `Table` object has an [API](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/Table.html#checkpoint-io.delta.kernel.engine.Engine-long-) to checkpoint the table.

```java
TransactionCommitResult commitResult = txn.commit(engine, dataActionsIterable);

if (commitResult.isReadyForCheckpoint()) {
  // Checkpoint the table
  Table.forPath(engine, tablePath).checkpoint(engine, commitResult.getVersion());
}
```

## Build a Delta connector for a distributed processing engine
Unlike simple applications that just read the table in a single process, building a connector for complex processing engines like Apache Spark™ and Trino can require quite a bit of additional effort. For example, to build a connector for an SQL engine you have to do the following

* Understand the APIs provided by the engine to build connectors and how Delta Kernel can be used to provide the information necessary for the connector + engine to operate on a Delta table.
* Decide what libraries to use to do computationally expensive operations like reading Parquet files, parsing JSON, computing expressions, etc. Delta Kernel provides all the extension points to allow you to plug in any library without having to understand all the low-level details of the Delta protocol.
* Deal with details specific to distributed engines. For example, 
  * Serialization of Delta table metadata provided by Delta Kernel. 
  * Efficiently transforming data read from Parquet into the engine in-memory processing format.

In this section, we are going to outline the steps needed to build a connector.

### Step 0: Validate the prerequisites
In the previous section showing how to read a simple table, we were briefly introduced to the [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/Engine.html). This is the main extension point where you can plug in your implementations of computationally-expensive operations like reading Parquet files, parsing JSON, etc. For the simple case, we were using a default implementation of the helper that works in most cases. However, for building a high-performance connector for a complex processing engine, you will very likely need to provide your own implementation using the libraries that work with your engine. So before you start building your connector, it is important to understand these requirements and plan for building your own engine.

Here are the libraries/capabilities you need to build a connector that can read the Delta table

* Perform file listing and file reads from your storage/file system.
* Read Parquet files in columnar data, preferably in an in-memory columnar format.
* Parse JSON data
* Read JSON files
* Evaluate expressions on in-memory columnar batches

For each of these capabilities, you can choose to build your own implementation or reuse the default implementation. 

### Step 1: Set up Delta Kernel in your connector project
In the Delta Kernel project, there are multiple dependencies you can choose to depend on.

1. Delta Kernel core APIs - This is a must-have dependency, which contains all the main APIs like Table, Snapshot, and Scan that you will use to access the metadata and data of the Delta table. This has very few dependencies reducing the chance of conflicts with any dependencies in your connector and engine. This also provides the [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/Engine.html) interface which allows you to plug in your implementations of computationally expensive operations, but it does not provide any implementation of this interface.
2. Delta Kernel default- This has a default implementation called [`DefaultEngine`](https://delta-io.github.io/delta/snapshot/kernel-defaults/java/io/delta/kernel/defaults/engine/DefaultEngine.html) and additional dependencies such as `Hadoop`. If you wish to reuse all or parts of this implementation, then you can optionally depend on this.

#### Set up Java projects
As discussed above, you can import one or both of the artifacts as follows:

```xml
<!-- Must have dependency -->
<dependency>
  <groupId>io.delta</groupId>
  <artifactId>delta-kernel-api</artifactId>
  <version>${delta-kernel.version}</version>
</dependency>

<!-- Optional depdendency -->
<dependency>
  <groupId>io.delta</groupId>
  <artifactId>delta-kernel-defaults</artifactId>
  <version>${delta-kernel.version}</version>
</dependency>
```

### Step 2: Build your own Engine
In this section, we are going to explore the [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/Engine.html) interface and walk through how to implement your own implementation so that you can plug in your connector/engine-specific implementations of computationally-intensive operations, threading model, resource management, etc.

> [!IMPORTANT]
> During the validation process, if you believe that all the dependencies of the default `Engine` implementation can work with your connector and engine, then you can skip this step and jump to Step 3 of implementing your connector using the default engine. If later you have the need to customize the helper for your connector, you can revisit this step.

#### Step 2.1: Implement the `Engine` interface

The [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/Engine.html) interface combines a bunch of sub-interfaces each of which is designed for a specific purpose. Here is a brief overview of the subinterfaces. See the API docs (Java) for a more detailed view.

```java
interface Engine {
  /**
   * Get the connector provided {@link ExpressionHandler}.
   * @return An implementation of {@link ExpressionHandler}.
  */
  ExpressionHandler getExpressionHandler();

  /**
   * Get the connector provided {@link JsonHandler}.
   * @return An implementation of {@link JsonHandler}.
   */
  JsonHandler getJsonHandler();

  /**
   * Get the connector provided {@link FileSystemClient}.
   * @return An implementation of {@link FileSystemClient}.
   */
  FileSystemClient getFileSystemClient();

  /**
   * Get the connector provided {@link ParquetHandler}.
   * @return An implementation of {@link ParquetHandler}.
   */
  ParquetHandler getParquetHandler();
}
```

To build your own `Engine` implementation, you can choose to either use the default implementations of each sub-interface or completely build every one from scratch.

```java
class MyEngine extends DefaultEngine {

  FileSystemClient getFileSystemClient() {
    // Build a new implementation from scratch
    return new MyFileSystemClient();
  }
  
  // For all other sub-clients, use the default implementations provided by the `DefaultEngine`.
}
```

Next, we will walk through how to implement each interface.

#### Step 2.2: Implement `FileSystemClient` interface

The [`FileSystemClient`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/FileSystemClient.html) interface contains basic file system operations like listing directories, resolving paths into a fully qualified path and reading bytes from files. Implementation of this interface must take care of the following when interacting with storage systems such as S3, Hadoop, or ADLS:

* Credentials and permissions: The connector must populate its `FileSystemClient` with the necessary configurations and credentials for the client to retrieve the necessary data from the storage system. For example, an implementation based on Hadoop's FileSystem abstractions can be passed S3 credentials via the Hadoop configurations.
* Decryption: If file system objects are encrypted, then the implementation must decrypt the data before returning the data.

#### Step 2.3: Implement `ParquetHandler`

As the name suggests, this interface contains everything related to reading and writing Parquet files. It has been designed such that a connector can plug in a wide variety of implementations, from a simple single-threaded reader to a very advanced multi-threaded reader with pre-fetching and advanced connector-specific expression pushdown. Let's explore the methods to implement, and the guarantees associated with them. 

##### Method `readParquetFiles(CloseableIterator<FileStatus> fileIter, StructType physicalSchema, java.util.Optional<Predicate> predicate)`

This [method]((https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/ParquetHandler.html#readParquetFiles-io.delta.kernel.utils.CloseableIterator-io.delta.kernel.types.StructType-)) takes as input `FileStatus`s which contains metadata such as file path, size etc. of the Parquet file to read. The columns to be read from the Parquet file are defined by the physical schema. To implement this method, you may have to first implement your own [`ColumnarBatch`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/ColumnarBatch.html) and [`ColumnVector`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/ColumnVector.html) which is used to represent the in-memory data generated from the Parquet files.

When identifying the columns to read, note that there are multiple types of columns in the physical schema (represented as a [`StructType`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/types/StructType.html)). 

* Data columns: Columns that are expected to be read from the Parquet file. Based on the `StructField` object defining the column, read the column in the Parquet file that matches the same name or field id. If the column has a field id (stored as `parquet.field.id` in the `StructField` metadata) then the field id should be used to match the column in the Parquet file. Otherwise, the column name should be used for matching.
* Metadata columns: These are special columns that must be populated using metadata about the Parquet file ([`StructField#isMetadataColumn`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/types/StructField.html#isMetadataColumn--) tells whether a column in `StructType` is a metadata column). To understand how to populate such a column, first match the column name against the set of standard metadata column name constants. For example, 
    * `StructFileld#isMetadataColumn()` returns true and the column name is `StructField.METADATA_ROW_INDEX_COLUMN_NAME`, then you have to a generate column vector populated with the actual index of each row in the Parquet file (that is, not indexed by the possible subset of rows returned after Parquet data skipping).

##### Requirements and guarantees
Any implementation must adhere to the following guarantees.

* The schema of the returned `ColumnarBatch`es must match the physical schema. 
  * If a data column is not found and the `StructField.isNullable = true`, then return a `ColumnVector` of nulls. Throw an error if it is not nullable.
* The output iterator must maintain ordering as the input iterator. That is, if `file1` is before `file2` in the input iterator, then columnar batches of `file1` must be before those of `file2` in the output iterator.

##### Method `writeParquetFiles(String directoryPath, CloseableIterator<FilteredColumnarBatch> dataIter, java.util.List<Column> statsColumns)`

This [method](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/ParquetHandler.html#writeParquetFiles-java.lang.String-io.delta.kernel.utils.CloseableIterator-java.util.List-) takes given data writes it into one or more Parquet files into the given directory. The data is given as an iterator of [FilteredColumnarBatches](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/FilteredColumnarBatch.html) which contains a [ColumnarBatch](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/ColumnarBatch.html) and an optional selection vector containing one entry for each row in `ColumnarBatch` indicating whether a row is selected or not selected. The `ColumnarBatch` also contains the schema of the data. This schema should be converted to Parquet schema, including any field IDs present [`FieldMetadata`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/types/FieldMetadata.html) for each column `StructField`.

There is also the parameter `statsColumns`, which is a hint to the Parquet writer on what set of columns to collect stats for each file. The statistics include `min`, `max` and `null_count` for each column in the `statsColumns` list. Statistics collection is optional, but when present it is used by Kernel to persist the stats as part of the Delta table commit. This will help read queries prune un-needed data files based on the query predicate.

For each written data file, the caller is expecting a [`DataFileStatus`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/utils/DataFileStatus.html) object. It contains the data file path, size, modification time, and optional column statistics.

#### Method `writeParquetFileAtomically(String filePath, CloseableIterator<FilteredColumnarBatch> data)`
This [method](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/ParquetHandler.html#writeParquetFiles-java.lang.String-io.delta.kernel.utils.CloseableIterator-java.util.List-) writes the given `data` into Parquet file at location `filePath`. The write is an atomic write i.e., either a Parquet file is created with all given content or no Parquet file is created at all. This should not create a file with partial content in it.

The default implementation makes use of [`LogStore`](https://github.com/delta-io/delta/blob/master/storage/src/main/java/io/delta/storage/LogStore.java) implementations from the [`delta-storage`](https://github.com/delta-io/delta/tree/master/storage) module to accomplish the atomicity. A connector that wants to implement their own version of `ParquetHandler` can take a look at the default implementation for details.
 
##### Performance suggestions
* The representation of data as `ColumnVector`s and `ColumnarBatch`es can have a significant impact on the query performance and it's best to read the Parquet file data directly into vectors and batches of the engine-native format to avoid potentially costly in-memory data format conversion. Create a Kernel `ColumnVector` and `ColumnarBatch` wrappers around the engine-native format equivalent classes.

#### Step 2.4: Implement `ExpressionHandler` interface
The [`ExpressionHandler`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/ExpressionHandler.html) interface has all the methods needed for handling expressions that may be applied on columnar data. 

##### Method `getEvaluator(StructType batchSchema, Expression expresion, DataType outputType)`

This [method](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/ExpressionHandler.html#getEvaluator-io.delta.kernel.types.StructType-io.delta.kernel.expressions.Expression-io.delta.kernel.types.DataType-) generates an object of type [`ExpressionEvaluator`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/expressions/ExpressionEvaluator.html) that can evaluate the `expression` on a batch of row data to produce a result of a single column vector. To generate this function, the `getEvaluator()` method takes as input the expression and the schema of the `ColumnarBatch`es of data on which the expressions will be applied. The same object can be used to evaluate multiple columnar batches of input with the same schema and expression the evaluator is created for.

##### Method `getPredicateEvaluator(StructType inputSchema, Predicate predicate)`
This [method](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/ExpressionHandler.html#createSelectionVector-boolean:A-int-int-) is for creating an expression evaluator for `Predicate` type expressions. The `Predicate` type expressions return a boolean value as output.

The returned object is of type [`PredicateEvaluator`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/expressions/PredicateEvaluator.html). This is a special interface for evaluating Predicate on input batch returns a selection vector containing one value for each row in input batch indicating whether the row has passed the predicate or not. Optionally it takes an existing selection vector along with the input batch for evaluation. The result selection vector is combined with the given existing selection vector and a new selection vector is returned. This mechanism allows running an input batch through several predicate evaluations without rewriting the input batch to remove rows that do not pass the predicate after each predicate evaluation. The new selection should be the same or more selective as the existing selection vector. For example, if a row is marked as unselected in the existing selection vector, then it should remain unselected in the returned selection vector even when the given predicate returns true for the row.

##### Method `createSelectionVector(boolean[] values, int from, int to)`
This [method](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/ExpressionHandler.html#createSelectionVector-boolean:A-int-int-) allows creating `ColumnVector` for boolean type values given as input. This allows the connector to maintain all `ColumnVector`s created in the desired memory format.

##### Requirements and guarantees
Any implementation must adhere to the following guarantees.

* Implementation must handle all possible variations of expressions. If the implementation encounters an expression type that it does not know how to handle, then it must throw a specific language-dependent exception.
  * Java: [NotSupportedException](https://docs.oracle.com/javaee/7/api/javax/resource/NotSupportedException.html) 
* The `ColumnarBatch`es on which the generated `ExpressionEvaluator` is going to be used are guaranteed to have the schema provided during generation. Hence, it is safe to bind the expression evaluation logic to column ordinals instead of column names, thus making the actual evaluation faster.

#### Step 2.5: Implement `JsonHandler`
[This](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/JsonHandler.html) engine interface allows the connector to use plug-in their own JSON handling code and expose it to the Delta Kernel.

##### Method `readJsonFiles(CloseableIterator<FileStatus> fileIter, StructType physicalSchema, java.util.Optional<Predicate> predicate)`

This [method](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/JsonHandler.html#readJsonFiles-io.delta.kernel.utils.CloseableIterator-io.delta.kernel.types.StructType-) takes as input `FileStatus`s of the JSON files and returns the data in a series of columnar batches. The columns to be read from the JSON file are defined by the physical schema, and the return batches must match that schema. To implement this method, you may have to first implement your own [`ColumnarBatch`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/ColumnarBatch.html)and [`ColumnVector`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/ColumnVector.html) which is used to represent the in-memory data generated from the JSON files.

When identifying the columns to read, note that there are multiple types of columns in the physical schema (represented as a [`StructType`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/types/StructType.html)). 
 
##### Method `parseJson(ColumnVector jsonStringVector, StructType outputSchema, java.util.Optional<ColumnVector> selectionVector)`

This [method](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/JsonHandler.html#parseJson-io.delta.kernel.data.ColumnVector-io.delta.kernel.types.StructType-) allows parsing a `ColumnVector` of string values which are in JSON format into the output format specified by the `outputSchema`. If a given column in `outputSchema` is not found, then a null value is returned. It optionally takes a selection vector which indicates what entries in the input `ColumnVector` of strings to parse. If an entry is not selected then a `null` value is returned as parsed output for that particular entry in the output.

##### Method `deserializeStructType(String structTypeJson)`

This [method](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/JsonHandler.html#deserializeStructType-java.lang.String-) allows parsing JSON encoded (according to [Delta schema serialization rules](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#schema-serialization-format)) `StructType` schema into a `StructType`. Most implementations of `JsonHandler` do not need to implement this method and instead use the one in the [default `JsonHandler`](https://delta-io.github.io/delta/snapshot/kernel-defaults/java/io/delta/kernel/defaults/engine/DefaultJsonHandler.html) implementation.

#### Method `writeJsonFileAtomically(String filePath, CloseableIterator<Row> data, boolean overwrite)`

This [method](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/JsonHandler.html#writeJsonFileAtomically-java.lang.String-io.delta.kernel.utils.CloseableIterator-boolean-) writes the given `data` into a JSON file at location `filePath`. The write is an atomic write i.e., either a JSON file is created with all given content or no Parquet file is created at all. This should not create a file with partial content in it.

The default implementation makes use of [`LogStore`](https://github.com/delta-io/delta/blob/master/storage/src/main/java/io/delta/storage/LogStore.java) implementations from the [`delta-storage`](https://github.com/delta-io/delta/tree/master/storage) module to accomplish the atomicity. A connector that wants to implement their own version of `JsonHandler` can take a look at the default implementation for details.

The implementation is expected to handle the serialization rules (converting the `Row` object to JSON string) as described in the [API Javadoc](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/JsonHandler.html#writeJsonFileAtomically-java.lang.String-io.delta.kernel.utils.CloseableIterator-boolean-).

#### Step 2.6: Implement `ColumnarBatch` and `ColumnVector`

[`ColumnarBatch`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/ColumnarBatch.html) and [`ColumnVector`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/ColumnVector.html) are two interfaces to represent the data read into memory from files. This representation can have a significant impact on query performance. Each engine likely has a native representation of in-memory data with which it applies data transformation operations. For example, in Apache Spark™, the row data is internally represented as `UnsafeRow` for efficient processing. So it's best to read the Parquet file data directly into vectors and batches of the native format to avoid potentially costly in-memory data format conversions. So the recommended approach is to build wrapper classes that extend the two interfaces but internally use engine-native classes to store the data. When the connector has to forward the columnar batches received from the kernel to the engine, it has to be smart enough to skip converting vectors and batches that are already in the engine-native format.

### Step 3: Build read support in your connector
In this section, we are going to walk through the likely sequence of Kernel API calls your connector will have to make to read a table. The exact timing of making these calls in your connector in the context of connector-engine interactions depends entirely on the engine-connector APIs and is therefore beyond the scope of this guide. However, we will try to provide broad guidelines that are likely (but not guaranteed) to apply to your connector-engine setup. For this purpose, we are going to assume that the engine goes through the following phases when processing a read/scan query - logical plan analysis, physical plan generation, and physical plan execution. Based on these broad characterizations, a typical control and data flow for reading a Delta table is going to be as follows:

<table>
  <tr>
   <td><strong>Step</strong>
   </td>
   <td><strong>Typical query phase when this step occurs</strong>
   </td>
  </tr>
  <tr>
   <td>Resolve the table snapshot to query
   </td>
   <td>Logical plan analysis phase when the plan's schema and other details need to be resolved and validated
   </td>
  </tr>
  <tr>
   <td>Resolve files to scan based on query parameters
   </td>
   <td>Physical plan generation, when the final parameters of the scan are available. For example: 
<ul>

<li>Schema of data to read after pruning away unused columns

<li>Query filters to apply after filter rearrangement
</li>
</ul>
   </td>
  </tr>
  <tr>
   <td>Distribute the file information to workers
   </td>
   <td>Physical plan execution, only if it is a distributed engine.
   </td>
  </tr>
  <tr>
   <td>Read the columnar data using the file information
   </td>
   <td>Physical plan execution, when the data is being processed by the engine 
   </td>
  </tr>
</table>

Let's understand the details of each step.

#### Step 3.1: Resolve the table snapshot to query
The first step is to resolve the consistent snapshot and the schema associated with it. This is often required by the connector/ engine to resolve and validate the logical plan of the scan query (if the concept of logical plan exists in your engine). To achieve this, the connector has to do the following.

* Resolve the table path from the query: If the path is directly available, then this is easy. Otherwise, if it is a query based on a catalog table (for example, a Delta table defined in Hive Metastore), then the connector has to resolve the table path from the catalog.
* Initialize the `Engine` object: Create a new instance of the `Engine` that you have chosen in [Step 2](#build-your-own Engine).
* Initialize the Kernel objects and get the schema: Assuming the query is on the latest available version/snapshot of the table, you can get the table schema as follows:

```java
import io.delta.kernel.*;
import io.delta.kernel.defaults.engine.*;

Engine myEngine = new MyEngine();
Table myTable = Table.forPath(myTablePath);
Snapshot mySnapshot = myTable.getLatestSnapshot(myEngine);
StructType mySchema = mySnapshot.getSchema(myEngine);
```

If you want to query a specific version of the table (that is, not the schema), then you can get the required snapshot as `myTable.getSnapshot(version)`.

#### Step 3.2: Resolve files to scan

Next, we need to build a Scan object using more information from the query. Here we are going to assume that the connector/engine has been able to extract the following details from the query (say, after optimizing the logical plan):

* Read schema: The columns in the table that the query needs to read. This may be the full set of columns or a subset of columns.
* Query filters: The filters on partitions or data columns that can be used skip reading table data.

To provide this information to Kernel, you have to do the following:

* Convert the engine-specific schema and filter expressions to Kernel schema and expressions: For schema, you have to create a `StructType` object. For the filters, you have to create an `Expression` object using all the available subclasses of `Expression`. 
* Build the scan with the converted information: Build the scan as follows:

```java
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;

StructType readSchema = ... ;  // convert engine schema
Predicate filterExpr = ... ;   // convert engine filter expression

Scan myScan = mySnapshot.buildScan(engine)
  .withFilter(myEngine, filterExpr)
  .withReadSchema(myEngine, readSchema)
  .build()

```

* Resolve the information required to file reads: The generated Scan object has two sets of information. 
  * Scan files: `myScan.getScanFiles()` returns an iterator of `ColumnarBatch`es. Each batch in the iterator contains rows and each row has information about a single file that has been selected based on the query filter.
  * Scan state: `myScan.getScanState()` returns a `Row` that contains all the information that is common across all the files that need to be read.

```java
Row myScanStateRow = myScan.getScanState();
CloseableIterator<FilteredColumnarBatch> myScanFilesAsBatches = myScan.getScanFiles();

while (myScanFilesAsBatches.hasNext()) {
  FilteredColumnarBatch scanFileBatch = myScanFilesAsBatches.next();

  CloseableIterator<Row> myScanFilesAsRows = scanFileBatch.getRows();
}
```

As we will soon see, reading the columnar data from a selected file will need to use both, the scan state row, and a scan file row with the file information.

##### Requirements and guarantees
Here are the details you need to ensure when defining this scan.

* The provided `readSchema` must be the exact schema of the data that the engine will expect when executing the query. Any mismatch in the schema defined during this query planning and the query execution will result in runtime failures. Hence you must build the scan with the readSchema only after the engine has finalized the logical plan after any optimizations like column pruning.
* When applicable (for example, with Java Kernel APIs), you have to make sure to call the close() method as you consume the `ColumnarBatch`es of scan files (that is, either serialize the rows or use them to read the table data).

#### Step 3.3: Distribute the file information to the workers
If you are building a connector for a distributed engine like Spark/Presto/Trino/Flink, then your connector has to send all the scan metadata from the query planning machine (henceforth called the driver) to task execution machines (henceforth called the workers). You will have to serialize and deserialize the scan state and scan file rows. It is the connector job to implement serialization and deserialization utilities for a [`Row`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/Row.html). If the connector wants to split reading one scan file into multiple tasks, it can add additional connector specific split context to the task. At the task, the connector can use its own Parquet reader to read the specific part of the file indicated by the split info.

##### Custom `Row` Serializer/Deserializer
Here are steps on how to build your own serializer/deserializer such that it will work with any [`Row`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/Row.html) of any schema. 

* Serializing
  * First serialize the row schema, that is, `StructType` object.
  * Then, use the schema to identify types of each column/ordinal in the `Row` and use that to serialize all the values one by one.

* Deserializing
  * Define your own class that extends the Row interface. It must be able to handle complex types like arrays, nested structs and maps.
  * First deserialize the schema.
  * Then, use the schema to deserialize the values and put them in an instance of your custom Row class.

```java
import io.delta.kernel.utils.*;

// In the driver where query planning is being done
Byte[] scanStateRowBytes = RowUtils.serialize(scanStateRow);
Byte[] scanFileRowBytes = RowUtils.serialize(scanFileRow);

// Optionally the connector adds a split info to the task (scan file, scan state) to
// split reading of a Parquet file into multiple tasks. The task gets split info
// along with the scan file row and scan state row.
Split split = ...; // connector specific class, not related to Kernel

// Send these over to the worker

// In the worker when data will be read, after rowBytes have been sent over
Row scanStateRow = RowUtils.deserialize(scanStateRowBytes);
Row scanFileRow = RowUtils.deserialize(scanFileRowBytes);
Split split = ... deserialize split info ...;
```

#### Step 3.4: Read the columnar data 
Finally, we are ready to read the columnar data. You will have to do the following:

* Read the physical data from Parquet file as indicated by the scan file row, scan state, and optionally the split info
* Convert the physical data into logical data of the table using the Kernel's APIs.

```java
Row scanStateRow = ... ;
Row scanFileRow = ... ;
Split split = ...;

// Additional option predicate such as dynamic filters the connector wants to
// pass to the reader when reading files.
Predicate optPredicate = ...;

// Get the physical read schema of columns to read from the Parquet data files
StructType physicalReadSchema =
  ScanStateRow.getPhysicalDataReadSchema(engine, scanStateRow);

// From the scan file row, extract the file path, size and modification metadata
// needed to read the file.
FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);

// Open the scan file which is a Parquet file using connector's own
// Parquet reader which supports reading specific parts (split) of the file.
// If the connector doesn't have its own Parquet reader, it can use the
// default Parquet reader provider which at the moment doesn't support reading
// a specific part of the file, but reads the entire file from the beginning.
CloseableIterator<ColumnarBatch> physicalDataIter =
  connectParquetReader.readParquetFile(
    fileStatus
    physicalReadSchema,
    split, // what part of the Parquet file to read data from
    optPredicate /* additional predicate the connector can apply to filter data from the reader */
  );

// Now the physical data read from the Parquet data file is converted to logical data
// the table represents.
// Logical data may include the addition of partition columns and/or
// subset of rows deleted
CloseableIterator<FilteredColumnarBatch> transformedData =
  Scan.transformPhysicalData(
    engine,
    scanState,
    scanFileRow,
    physicalDataIter));
```

* Resolve the data in the batches: Each [`FilteredColumnarBatch`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/FilteredColumnarBatch.html) has two components:
    * Columnar batch (returned by `FilteredColumnarBatch.getData()`): This is the data read from the files having the schema matching the readSchema provided when the Scan object was built in the earlier step.
    * Optional selection vector (returned by `FilteredColumnarBatch.getSelectionVector()`): Optionally, a boolean vector that will define which rows in the batch are valid and should be consumed by the engine.

If the selection vector is present, then you will have to apply it to the batch to resolve the final consumable data. 

* Convert to engine-specific data format: Each connector/engine has its own native row / columnar batch formats and interfaces. To return the read data batches to the engine, you have to convert them to fit those engine-specific formats and/or interfaces. Here are a few tips that you can follow to make this efficient.
  * Matching the engine-specific format: Some engines may expect the data in an in-memory format that may be different from the data produced by `getData()`. So you will have to do the data conversion for each column vector in the batch as needed. 
  * Matching the engine-specific interfaces: You may have to implement wrapper classes that extend the engine-specific interfaces and appropriately encapsulate the row data.

For best performance, you can implement your own Parquet reader and other `Engine` implementations to make sure that every `ColumnVector` generated is already in the engine-native format thus eliminating any need to convert.

Now you should be able to read the Delta table correctly.

### Step 4: Build append support in your connector
In this section, we are going to walk through the likely sequence of Kernel API calls your connector will have to make to append data to a table. The exact timing of making these calls in your connector in the context of connector-engine interactions depends entirely on the engine-connector APIs and is, therefore, beyond the scope of this guide. However, we will try to provide broad guidelines that are likely (but not guaranteed) to apply to your connector-engine setup. For this purpose, we are going to assume that the engine goes through the following phases when processing a write query - logical plan analysis, physical plan generation, and physical plan execution. Based on these broad characterizations, a typical control and data flow for reading a Delta table is going to be as follows:

<table>
  <tr>
   <td><strong>Step</strong>
   </td>
   <td><strong>Typical query phase when this step occurs</strong>
   </td>
  </tr>
  <tr>
   <td>Determine the schema of the data that needs to be written to the table. Schema is derived from the existing table or from the parent operation of the `write` operator in the query plan when the table doesn't exist yet.
   </td>
   <td>Logical plan analysis phase when the plan's schema (`write` operator schema matches the table schema, etc.) and other details need to be resolved and validated.
   </td>
  </tr>
  <tr>
   <td>Determine the physical partitioning of the data based on the table schema and partition columns either from the existing table or from the query plan (for new tables)
   </td>
   <td>Physical plan generation, where the number of writer tasks, data schema and partitioning is determined</td>
  </tr>
  <tr>
   <td>Distribute the writer tasks definitions (which include the transaction state) to workers. 
   </td>
   <td>Physical plan execution, only if it is a distributed engine.
   </td>
  </tr>
  <tr>
   <td>Tasks write the data to data files and send the data file info to the driver.
   </td>
   <td>Physical plan execution, when the data is actually written to the table location
   </td>
  </tr>
  <tr>
   <td>Finalize the query. Here, all the info of the data files written by the tasks is aggregated and committed to the transaction created at the beginning of the physical execution.
   </td>
   <td>Finalize the query. This happens on the driver where the query has started.
   </td>
  </tr>
</table>

Let's understand the details of each step.

#### Step 4.1: Determine the schema of the data that needs to be written to the table
The first step is to resolve the output data schema. This is often required by the connector/ engine to resolve and validate the logical plan of the  query (if the concept of logical plan exists in your engine). To achieve this, the connector has to do the following. At a high level query plan is a tree of operators where the leaf-level operators generate or read data from storage/tables and feed it upwards towards the parent operator nodes. This data transfer happens until it reaches the root operator node where the query is finalized (either the results are sent to the client or data is written to another table).

* Create the `Table` object
* From the `Table` object try to get the schema.
  * If the table is not found
    * the query includes creating the table (e.g., `CREATE TABLE AS` SQL query);
      * the schema is derived from the operator above the `write` that feeds the data to the `write` operator.
    * the query doesn't include creating new table, an exception is thrown saying the table is not found
  * If the table already exists
    * get the schema from the table and check if it matches the schema of the `write` operator. If not throw an exception. 
* Create a `TransactionBuilder` - this basically begins the steps of transaction construction.
```java
import io.delta.kernel.*;
import io.delta.kernel.defaults.engine.*;

Engine myEngine = new MyEngine();
Table myTable = Table.forPath(myTablePath);

StructType writeOperatorSchema = // ... derived from the query operator tree ...
StructType dataSchema;
boolean isNewTable = false;

try {
  Snapshot mySnapshot = myTable.getLatestSnapshot(myEngine);
  dataSchema = mySnapshot.getSchema(myEngine);

  // .. check dataSchema and writeOperatorSchema match ...
} catch(TableNotFoundException e) {
  isNewTable = true;
  dataSchema = writeOperatorSchema;
}

TransactionBuilder txnBuilder =
  myTable.createTransactionBuilder(
    myEngine,
    "Examples", /* engineInfo - connector can add its own identifier which is noted in the Delta Log */ 
    Operation /* What is the operation we are trying to perform? This is noted in the Delta Log */
  );

if (isNewTable) {
  // For a new table set the table schema in the transaction builder
  txnBuilder = txnBuilder.withSchema(engine, dataSchema)
}

```

#### Step 4.2: Determine the physical partitioning of the data based on the table schema and partition columns

Partition columns are found either from the query (for new tables, the query defines the partition columns) or from the existing table.

```java
TransactionBuilder txnBuilder = ... from the last step ...
Transaction txn;

List<String> partitionColumns = ...
if (newTable) {
  partitionColumns = ... derive from the query parameters (ex. PARTITION BY clause in SQL) ...
  txnBuilder = txnBuilder.withPartitionColumns(engine, partitionColumns);
  txn = txnBuilder.build(engine);
} else {
  txn = txnBuilder.build(engine);
  partitionColumns = txn.getPartitionColumns(engine);
}
```

At the end of this step, we have the `Transaction` and schema of the data to generate and its partitioning.

#### Step 4.3: Distribute the writer tasks definitions (which include the transaction state) to workers
If you are building a connector for a distributed engine like Spark/Presto/Trino/Flink, then your connector has to send all the writer metadata from the query planning machine (henceforth called the driver) to task execution machines (henceforth called the workers). You will have to serialize and deserialize the transaction state. It is the connector job to implement serialization and deserialization utilities for a [`Row`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/Row.html). More details on a custom `Row` SerDe are found [here](#custom-row-serializerdeserializer).

```java
Row txnState = txn.getState(engine);

String jsonTxnState = serializeToJson(txnState);
```

#### Step 4.4: Tasks write the data to data files and send the data file info to the driver.
In this step (which is executed on the worker nodes inside each task):
* Deserialize the transaction state
* Writer operator within the task gets the data from its parent operator.
* The data is converted into a `FilteredColumnarBatch`. Each [`FilteredColumnarBatch`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/data/FilteredColumnarBatch.html) has two components:
    * Columnar batch (returned by `FilteredColumnarBatch.getData()`): This is the data read from the files having the schema matching the readSchema provided when the Scan object was built in the earlier step.
    * Optional selection vector (returned by `FilteredColumnarBatch.getSelectionVector()`): Optionally, a boolean vector that will define which rows in the batch are valid and should be consumed by the engine.
* The connector can create `FilteredColumnBatch` wrapper around data in its own in-memory format.
* Check if the data is partitioned or not. If not partitioned, partition the data by partition values.
* For each partition generate the map of the partition column to the partition value
* Use Kernel to convert the partitioned data into physical data that should go into the data files
* Write the physical data into one or more data files.
* Convert data file statues into a Delta log actions
* Serialize the Delta log action `Row` objects and send them to the driver node

```
Row txnState = ... deserialize from JSON string sent by the driver ...

CloseableIterator<FilteredColumnarBatch> data = ... generate data ...

// If the table is un-partitioned then this is an empty map
Map<String, Literal> partitionValues = ... prepare the partition values ...


// First transform the logical data to physical data that needs to be written
// to the Parquet files
CloseableIterator<FilteredColumnarBatch> physicalData =
  Transaction.transformLogicalData(engine, txnState, data, partitionValues);

// Get the write context
DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, partitionValues);

// Now write the physical data to Parquet files
CloseableIterator<DataFileStatus> dataFiles =
  engine.getParquetHandler()
    .writeParquetFiles(
      writeContext.getTargetDirectory(),
      physicalData,
      writeContext.getStatisticsColumns());

// Now convert the data file status to data actions that needs to be written to the Delta table log
CloseableIterator<Row> partitionDataActions =
  Transaction.generateAppendActions(
    engine,
    txnState,
    dataFiles,
    writeContext);

.... serialize `partitionDataActions` and send them to driver node
```

#### Step 4.5: Finalize the query.
At the driver node, the delta log actions from all the tasks are received and committed to the transaction. The tasks send the Delta log actions as a serialized JSON and deserialize them back to `Row` objects.

```
// Create a iterable out of the data actions. If the contents are too big to fit in memory,
// the connector may choose to write the data actions to a temporary file and return an
// iterator that reads from the file.
CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(
	toCloseableIterator(dataActions.iterator()));

// Commit the transaction.
TransactionCommitResult commitResult = txn.commit(engine, dataActionsIterable);

// Optional step
if (commitResult.isReadyForCheckpoint()) {
  // Checkpoint the table
  Table.forPath(engine, tablePath).checkpoint(engine, commitResult.getVersion());
}
```

Thats it. Now you should be able to append data to Delta tables using the Kernel APIs.


## Migration guide
Kernel APIs are still evolving and new features are being added. Kernel authors try to make the API changes backward compatible as much as they can with each new release, but sometimes it is hard to maintain the backward compatibility for a project that is evolving rapidly.

This section provides guidance on how to migrate your connector to the latest version of Delta Kernel. With each new release the [examples](https://github.com/delta-io/delta/tree/master/kernel/examples) are kept up-to-date with the latest API changes. You can refer to the examples to understand how to use the new APIs.

### Migration from Delta Lake version 3.1.0 to 3.2.0
Following are API changes in Delta Kernel 3.2.0 that may require changes in your connector.

#### Rename `TableClient` to [`Engine`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/engine/Engine.html)
The `TableClient` interface has been renamed to `Engine`. This is the most significant API change in this release. The `TableClient` interface name is not exactly representing the functionality it provides. At a high level it provides capabilities such as reading Parquet files, JSON files, evaluating expressions on data and file system functionality. These are basically the heavy lift operations that Kernel depends on as a separate interface to allow the connectors to substitute their own custom implementation of the same functionality (e.g. custom Parquet reader). Essentially, these functionalities are the core of the `engine` functionalities. By renaming to `Engine`, we are representing the interface functionality with a proper name that is easy to understand.

The `DefaultTableClient` has been renamed to [`DefaultEngine`](https://delta-io.github.io/delta/snapshot/kernel-defaults/java/io/delta/kernel/defaults/engine/DefaultEngine.html).

#### [`Table.forPath(Engine engine, String tablePath)`](https://delta-io.github.io/delta/snapshot/kernel-api/java/io/delta/kernel/Table.html#forPath-io.delta.kernel.engine.Engine-java.lang.String-) behavior change
Earlier when a non-existent table path is passed, the API used to throw `TableNotFoundException`. Now it doesn't throw the exception. Instead, it returns a `Table` object. When trying to get a `Snapshot` from the table object it throws the `TableNotFoundException`.

#### [`FileSystemClient.resolvePath`](https://delta-io.github.io/delta/snapshot/kernel-defaults/java/io/delta/kernel/defaults/engine/DefaultFileSystemClient.html#resolvePath-java.lang.String-) behavior change
Earlier when a non-existent path is passed, the API used to throw `FileNotFoundException`. Now it doesn't throw the exception. It still resolves the given path into a fully qualified path.
