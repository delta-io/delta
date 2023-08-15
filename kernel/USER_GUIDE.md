## Delta Kernel User Guide

## What is Delta Kernel?
Delta Kernel is a library for operating on Delta tables. Specifically, it provides simple and narrow APIs for reading and writing to Delta tables without the need to understand the [Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) details. You can use this library to do the following:

* Read Delta tables from your apps.
* Build a connector for a distributed engine like [Apache Spark](https://github.com/apache/spark), [Apache Flink](https://github.com/apache/flink) or [Trino](https://github.com/trinodb/trino) for reading massive Delta tables.

In this user guide, we are going to walk through the following: 
* [How to set up Delta Kernel with your project and read Delta tables using the default table client](#read-a-delta-table-in-a-single-process).
* [How to build a Delta connector for distributed engines with custom table client](#build-a-delta-connector-for-a-distributed-processing-engine).
* How to migrate existing Delta connectors to use Delta Kernel (coming soon).

## Read a Delta table in a single process
In this section, we will walk through how to build a very simple single-process Delta connector that can read a Delta table using the default [`TableClient`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/client/TableClient.html) implementation provided by Delta Kernel.

You can either write this code yourself in your project, or you can use the [examples](https://github.com/delta-io/delta/kernel/examples) present in the Delta code repository.

### Step 1: Set up Delta Kernel for your project
You need to `io.delta:delta-kernel.api` and `io.delta:delta-kernel-default` dependencies. Following is an example Maven `pom` file dependency list.

```xml
<dependencies>
  <dependency>
    <groupId>io.delta</groupId>
    <artifactId>delta-kernel-api</artifactId>
    <version>${delta-kernel.version}</version>
  </dependency>

  <dependency>
    <groupId>io.delta</groupId>
    <artifactId>delta-kernel-default</artifactId>
    <version>${delta-kernel.version}</version>
  </dependency>
</dependencies>
```

### Step 2: Full scan on a Delta table
The main entry point is [`io.delta.kernel.Table`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/Table.html) which is a programmatic representation of a Delta table. Say you have a Delta table at the directory `myTablePath`. You can create a `Table` object as follows:

```java
import io.delta.kernel.*;
import io.delta.kernel.defaults.*';
import org.apache.hadoop.conf.Configuration;

String myTablePath = <my-table-path>; // fully qualified table path. Ex: file:/user/tables/myTable
Configuration hadoopConf = new Configuration();
TableClient myTableClient = DefaultTableClient.create(hadoopConf);
Table myTable = Table.forPath(myTablePath);
```

Note the default [`TableClient`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/TableClient.html) we are creating to bootstrap the `myTable` object. This object allows you to plugin your own libraries for computationally intensive operations like Parquet file reading, JSON parsing, etc. You can ignore it for now. We will discuss more about this later when we discuss how to build more complex connectors for distributed processing engines. 

From this `myTable` object you can create a [`Snapshot`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/Snapshot.html) object which represents the consistent state (aka a snapshot consistency) in a specific version of the table.  

```java
Snapshot mySnapshot = myTable.getLatestSnapshot(myTableClient);
```

Now that we have a consistent snapshot view of the table, we can query more details about the table. For example, you can get the version and schema of this snapshot.

```java
long version = mySnapshot.getVersion(myTableClient);
StructType tableSchema = mySnapshot.getSchema(myTableClient);
```

Next, to read the table data, we have to *build* a [`Scan`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/Scan.html) object. In order to builde a `Scan` object, create a [`ScanBuilder`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/ScanBuilder.html) object which allows optional selection of subset of columns to read and/or set the query filter. For now ignore these optional settings.

```java
Scan myScan = mySnapshot.getScanBuilder(myTableClient).build()

// Common information about scan for all data files to read.
Row scanState = myScan.getScanState(myTableClient)

// Information about list of scan file to read
CloseableIterator<ColumnarBatch> scanFiles = myScan.getScanFiles(myTableClient)
```

This [Scan](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/Scan.html) object has all the necessary metadata to start reading the table. There are two crucial pieces of information needed for reading data from one file in the table. 

* `myScan.getScanFiles(TableClient)`:  Returns a set of rows (represented as an iterator of `ColumnarBatche`s, more on than later) where each row has information about a single file containing the table data.
* `myScan.getScanState(TableClient)`: Returns the snapshot-level information needed for reading any file. Note that this is a single row and common to all scan files.

To read the data, you have to call[`ScanFile.readData()`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/Scan.html#readData-io.delta.kernel.client.TableClient-io.delta.kernel.data.Row-io.delta.kernel.utils.CloseableIterator-java.util.Optional-) with the scan state and each of the scan files (that is, each row returned by `myScan.getScanFiles()`). Here is an example of reading all the table data in a single thread. 

```java
CloserableIterator<ColumnarBatch> fileIter = scanObject.getScanFiles(myTableClient);

Row scanStateRow = scanObject.getScanState(myTableClient);

while(fileIter.hasNext()) {
  ColumnarBatch scanFileColumnarBatch = fileIter.next();
  CloseableIterator<Row> scanFileRows = scanFileColumnarBatch.getRows();

  CloseableIterator<DataReadResult> dataResultIter = Scan.readData(
    myTableClient,
    scanStateRow,
    scanFileRows,
    Optional.empty() /* filter */
  );

  while(dataResultIter.hasNext()) {
    DataReadResult dataReadResult = dataResultIter.next();

    ColumnarBatch dataBatch = dataReadResult.getData();

    // Not all rows in `dataBatch` are in the selected output. An optional selection vector
    // determines whether a row with specific row index is in the final output or not.
    Optional<ColumnVector> selectionVector = dataReadResult.getSelectionVector();

    // access the data for column at ordinal 0
    ColumnVector column0 = dataBatch.getColumnVector(0);
    for (int rowIndex = 0; rowIndex < column0.getSize(); rowIndex++) {
      // check if the row is selected or not
      if (selectionVector.isEmpty() || selectionVector.get().getBoolean(rowIndex)) {
        // Assuming the column type is String.
        // If it is a different type, call the relevant function on the `ColumnVector`
        System.out.println(column0.getString(rowIndex));
      }
    }

    // access the data for column at ordinal 1
    ColumnVector column1 = dataBatch.getColumnVector(1);
    for (int rowIndex = 0; rowIndex < column1.getSize(); rowIndex++) {
      // check if the row is selected or not
      if (selectionVector.isEmpty() || selectionVector.get().getBoolean(rowIndex)) {
        // Assuming the column type is Long.
        // If it is a different type, call the relevant function on the `ColumnVector`
        System.out.println(column1.getLong(rowIndex));
      }
    }
	// .. more ..
  }
}
```

Few working examples to read Delta tables within a single process are available [here](https://github.com/delta-io/delta/tree/master/kernel/examples).

#### Important Note
* All the Delta protocol-level details are encoded in the rows returned by `Scan.getScanFiles` API, but you do not have to understand them in order to read the table data correctly. You can mostly treat these rows as opaque objects and pass them correctly to the `Scan.readData()` method. In future, there will be more information (that is, more columns) added to these rows, but your code will not have to change to accommodate those changes. This is the major advantage of the abstractions provided by Delta Kernel.

* Observe that same `TableClient` instance `myTableClient` is passed multiple times whenever a call to Delta Kernel API is made. The reason for passing this instance for every call is because it is the connector context, it should maintained outside of the Delta Kernel APIs to give the connector control over the `TableClient`.

### Step 3: Improve scan performance with file skipping
We have explored how to do a full table scan. But the real advantage of using the Delta format is when you can skip files using your query filters. To make this possible, Delta Kernel provides an expression framework to encode your filters and provide them to Delta Kernel to skip files during the scan file generation. For example, say your table is partitioned by `columnX`, you want to query only the partition `columnX=1`. You can generate the expression and use it to build the scan as follows:

```java
import io.delta.kernel.expressions.*;

import io.delta.kernel.defaults.client.*

TableClient myTableClient = DefaultTableClient.create(new Configuration());

Expression filter = new Equals(new Column("columnX"), Literal.forInteger(1));
Scan myFilteredScan = mySnapshot.buildScan(tableClient)
  .withFilter(myTableClient, filter)
  .build()

// Subset of the given filter that is not guaranteed to be satisfied by
// Delta Kernel when it returns data.
// This filter is used by Delta Kernel to do data skipping as much possible. 
// Connector should use this filter on top of the data returned by
// Delta Kernel in order for further filtering.
Expression remainingFilter = myFilteredScan.getRemainingFilter();
```

The scan files returned by  `myFilteredScan.getScanFiles(myTableClient)` will have rows representing files only of the required partition. Similarly, you can provide filters for non-partition columns, and if the data in the table is well clustered by those columns, then Delta Kernel will be able to skip files as well as much as possible.

## Build a Delta connector for a distributed processing engine
Unlike simple apps that just read the table in a single process, building a connector for complex processing engines like Apache Spark and Trino can require quite a bit of additional effort. For example, to build a connector for a SQL engine you have to do the following

* Understand the APIs provided by the engine to build connectors and how Delta Kernel can be used to provide the information necessary for the connector + engine to operate on a Delta table.
* Decide what libraries to use to do computationally expensive operations like reading Parquet files, parsing JSON, computing expressions, etc. Delta Kernel provides all the extension points to allow you to plug in any library without having to understand all the low-level details of the Delta protocol.
* Deal with details specific to distributed engines. For example, 
  * Serialization of Delta table metadata provided by Delta Kernel. 
  * Efficiently transforming data read from Parquet into the engine in-memory processing format.

In this section, we are going to outline the steps needed to build a connector.

### Step 0: Validate the prerequisites
In the previous section showing how to read a simple table, we were briefly introduced to the [`TableClient`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/TableClient.html). This is the main extension point where you can plug in your implementations of computationally-expensive operations like reading Parquet files, parsing JSON, etc. For the simple case, we were using a default implementation of the helper that works in most cases. However, for building a high-performance connector for a complex processing engine, you will very likely need to provide your own implementation using the libraries that work with your engine. So before you start building your connector, it is important to understand these requirements, and plan for building your own table client.

Here are the libraries/capabilities you need to build a connector that can read Delta table

* Perform file listing and file reads from your storage / file system.
* Read Parquet files. Specifically, 
    * Read the Parquet file footer
    * Read the Parquet columnar data, preferably in an in-memory columnar format.
* Parse JSON data
* Read JSON files
* Evaluate expressions on in-memory columnar batches

For each of these capabilities, you can choose to build your own implementation, or reuse the default implementation. 

### Step 1: Set up Delta Kernel in your connector project
In the Delta Kernel project, there are multiple dependencies you can choose to depend on.

1. Delta Kernel core APIs - This is a must-have dependency, which contains all the main APIs like Table, Snapshot and Scan that you will use to access the metadata and data of the Delta table. This has very few dependencies reducing the chance of conflicts with any dependencies in your connector and engine. This also provides the [`TableClient`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/TableClient.html) interface which allows you to plug in your implementations of computationally-expensive operations, but it does not provide any implementation of this interface. 
2. Delta Kernel default [`TableClient`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//index.html) - This has a default implementation called [`DefaultTableClient`](#TODO-Add link) and additional dependencies such as `Hadoop`. If you wish to reuse all or parts of this implementation, then you can optionally depend on this. 

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
  <artifactId>delta-kernel-default</artifactId>
  <version>${delta-kernel.version}</version>
</dependency>
```

### Step 2: Build your own TableClient
In this section, we are going to explore the [`TableClient`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/TableClient.html) interface and walk through how to implement your own implementation so that you can plug in your connector/engine-specific implementations of computationally-intensive operations, threading model, resource management, etc.

#### Important Note
During the validation process, if you believe that all the dependencies of the default `TableClient` implementation can work with your connector and engine, then you can skip this step and jump to Step 3 of implementing your connector using the default client. If later you have the need to customize the helper for your connector, you can revisit this step.

#### Step 2.1: Implement the TableClient interface

The [`TableClient`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/client/TableClient.html) interface combines a bunch of sub-interfaces each of which is designed for a specific purpose. Here is a brief overview of the subinterfaces. See the API docs (Java) for the more detailed view.

```java
interface TableClient {
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

To build your own `TableClient` implementation, you can choose to either use the default implementations of each sub-interface, or completely build every one from scratch.

```java
class MyTableClient extends DefaultTableClient {

  FileSystemClient getFileSystemClient() {
    // Build a new implementation from scratch
    return new MyFileSystemClient();
  }
  
  // For all other sub-clients, use the default implementations provided by the `DefaultTableClient`.
}
```

Next we will walk through how to implement each interface.

#### Step 2.2: Implement [`FileSystemClient`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/FileSystemClient.html) interface

The [`FileSystemClient`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/FileSystemClient.html) interface contains basic file system operations like listing directories and reading bytes from files. Implementation of this interface must take care of the following when interacting with the storage systems such as S3, Hadoop or ADLS:

* Credentials and permissions: Connector must populate its `FileSystemClient` with necessary configurations and credentials for the client to retrieve the necessary data from the storage system. For example, an implementation based on Hadoop's FileSystem abstractions can be passed S3 credentials via the Hadoop configurations.
* Decryption: If file system objects are encrypted, then the implementation must decrypt the data before returning the data.

#### Step 2.3: Implement [`ParquetReader`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/ParquetHandler.html)

As the name suggests, this interface contains everything related to reading Parquet files. It has been designed such that a connector can plug in a wide variety of implementations, from a simple single-threaded reader, to a very advanced multi-threaded reader with pre-fetching and advanced connector-specific expression pushdown. Let's explore the methods to implement, and the guarantees associated with them. 

##### Method [`contextualizeFileReads()`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/FileHandler.html#contextualizeFileReads-io.delta.kernel.utils.CloseableIterator-io.delta.kernel.expressions.Expression-)

This method associates the information of a to-be-read file to a connector-specific context object, called `FileReadContext`. Implementations can use this method to inject connector-specific information to optimize the file read. For example, if you want split reading a 256 MB Parquet file (say, `file1.parquet`) into 2 chunks of size 128 MB, then you can create two successive entries `fileReadContext1(file1.parquet)` and `fileReadContext2(file1.parquet)` where the context objects created by this method contains the byte ranges to read. This byte ranges will be used by the next method to do the necessary partial reads of the file `file1.parquet`.

##### Requirements and guarantees

Any implementation must adhere to the following guarantees.
* The output iterator must maintain the same ordering as the input iterator. For example, if file1 is before file2 in the input iterator, then all the contexts associated with file1 must be before those of file2 in the output iterator.

##### Method [`readParquetFiles()`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/ParquetHandler.html#readParquetFiles-io.delta.kernel.utils.CloseableIterator-io.delta.kernel.types.StructType-)

This method takes as input information as `FileReadContext`s that were created using the `contextualizeFileReader` method above and returns the data in a series of columnar batches. The columns to be read from the Parquet file are defined by the physical schema, and the return batches must match that schema. To implement this method, you may have to first implement your own [`ColumnarBatch`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/data/ColumnarBatch.html) and [`ColumnVector`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/data/ColumnVector.html) which is used to represent the in-memory data generated from the Parquet files.

When identifying the columns to read, note that there are multiple types of columns in the physical schema (represented as a [`StructType`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/types/StructType.html)). 

* Data columns: Columns that are expected to be read from the Parquet file. Based on the `StructField` object defining the column, there are two cases of mapping the column description to the right column in the file. 
    * If the optional `StructField.fieldId` is defined, then read the column in the Parquet file which has the same field id.
    * Otherwise, read the column in the Parquet file that matches the same name.
* Metadata columns: These are special columns that must be populated using metadata about the Parquet file ([`StructField#isMetadataColumn`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/types/StructField.html#isMetadataColumn--) tells whether a column in `StructType` is a metadata column). To understand how to populate such a column, first match the column name against the set of standard metadata column name constants. For example, 
    * `StructFileld#isMetadataColumn()` returns true and the column name is `StructField.ROW_INDEX_COLUMN_NAME`, then you have to a generate column vector populated with the actual index of each row in the Parquet file (that is, not indexed by the possibly subset of rows return after Parquet data skipping).

##### Requirements and guarantees
Any implementation must adhere to the following guarantees.

* The schema of the returned `ColumnarBatch`es must match the physical schema. 
  * If a data column is not found and the `StructField.isNullable = true`, then return a `ColumnVector` of nulls. Throw error if it is not nullable.
* The output iterator must maintain ordering as the input iterator. That is, if `file1` is before `file2` in the input iterator, then columnar batches of `file1` must be before those of `file2` in the output iterator.

##### Performance suggestions
* The representation of data as `ColumnVector`s and `ColumnarBatch`es can have a significant impact on the query performance and it's best to read the Parquet file data directly into vectors and batches of the engine-native format to avoid potentially costly in-memory data format conversion. Create a Kernel `ColumnVector` and `ColumnarBatch` wrappers around the engine-native format equivalent classes.

#### Step 2.4: Implement [`ExpressionHandler`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/client/ExpressionHandler.htm)
The [`ExpressionHandler`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/client/ExpressionHandler.html) interface has all the methods needed for handling expressions that may be applied on columnar data. 

##### Method [`getEvaluator(StructType batchSchema, Expression expresion)`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/ExpressionHandler.html#getEvaluator-io.delta.kernel.types.StructType-io.delta.kernel.expressions.Expression-)

This method generates an object of type [`ExpressionEvaluator`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/expressions/ExpressionEvaluator.html) that can evaluate the `expression` on a batch of row data to produce a result of a single column vector. To generate this function, the `getEvaluator()` method takes as input the expression and the schema of the `ColumnarBatch`es of data on which the expressions will be applied. Same object can be used to evaluate multiple columnar batches of input with the same schema and expression the evaluator is created for.

##### Requirements and guarantees
Any implementation must adhere to the following guarantees.

* Implementation must handle all possible variations of expressions. If the implementation encounters an expression type that it does not know how to handle, then it must throw a specific language-dependent exception.
  * Java: [NotSupportedException](https://docs.oracle.com/javaee/7/api/javax/resource/NotSupportedException.html) 
* The `ColumnarBatch`es on which the generated `ExpressionEvaluator` is going to be used are guaranteed to have the schema provided during generation. Hence, it is safe to bind the expression evaluation logic to column ordinals instead of column names, thus making the actual evaluation faster.

#### Step 2.5: Implement [`JsonHandler`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/JsonHandler.html)
This client interface allows connector to use plug-in their own JSON handing code and expose it to the Delta Kernel.

##### Method [`contextualizeFileReads()`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/FileHandler.html#contextualizeFileReads-io.delta.kernel.utils.CloseableIterator-io.delta.kernel.expressions.Expression-)
This method associates the information of a to-be-read file to a connector-specific context object, called `FileReadContext`. Implementations can use this method to inject connector-specific information to optimize the file read.

###### Requirements and guarantees
Any implementation must adhere to the following guarantees.

* The output iterator must maintain the same ordering as the input iterator. For example, if `file1` is before `file2` in the input iterator, then all the contexts associated with `file1` must be before those of `file2` in the output iterator.

##### Method [`readJsonFiles()`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/JsonHandler.html#readJsonFiles-io.delta.kernel.utils.CloseableIterator-io.delta.kernel.types.StructType-)

This method takes as input information as `FileReadContext`s that were created using the `contextualizeFileReader` method above and returns the data in a series of columnar batches. The columns to be read from the JSON file are defined by the physical schema, and the return batches must match that schema. To implement this method, you may have to first implement your own [`ColumnarBatch`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/data/ColumnarBatch.html)and [`ColumnVector`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/data/ColumnVector.html) which is used to represent the in-memory data generated from the JSON files.

When identifying the columns to read, note that there are multiple types of columns in the physical schema (represented as a [`StructType`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/types/StructType.html)). 
 
#### Method [`parseJson`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/client/JsonHandler.html#parseJson-io.delta.kernel.data.ColumnVector-io.delta.kernel.types.StructType-)

This method allows parsing a `ColumnVector` of string values which are in JSON format into the output format specified by the `outputSchema`. If a given column column `outputSchema` is not found, then a null value is returned.

#### Step 2.6: Implement [`ColumnarBatch`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/data/ColumnarBatch.html) and [`ColumnVector`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api//io/delta/kernel/data/ColumnVector.html)

`ColumnarBatch` and `ColumnVector` are two interfaces to represent the data read into memory from files. This representation can have a significant impact on query performance. Each engine likely has a native representation of in-memory data with which it applies data transformations operations. For example, in Apache Spark, the row data is internally represented as `UnsafeRow` for efficient processing. So it's best to read the Parquet file data directly into vectors and batches of the native format to avoid potentially costly in-memory data format conversions. So the recommended approach is to build wrapper classes that extend the two interfaces but internally use engine-native classes to store the data. When the connector has to forward the columnar batches received from the kernel to the engine, it has to be smart enough to skip converting vectors and batches that are already in the engine-native format.

### Step 3: Build read support in your connector
In this section, we are going to walk through the likely sequence of Kernel API calls your connector will have to make to read a table. The exact timing of making these calls in your connector in the context of connector-engine interactions depends entirely on the engine-connector APIs and is therefore beyond the scope of this guide. However, we will try to provide broad guidelines that are likely (but not guaranteed) to apply to your connector-engine setup. For this purpose, we are going to assume that the engine goes through the following phases when processing a read/scan query - logical plan analysis, physical plan generation, physical plan execution. Based on this broad characterizations, a typical control and data flow for reading a Delta table is going to be as follows:

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
   <td>Logical plan analysis phase when the plan's schema and other details need to resolved and validated
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

* Resolve the table path from the query: If the path is directly available, then this is easy. Else if it is a query based on a catalog table (for example, a Delta table defined in Hive Metastore), then the connector has to resolve the table path from the catalog.
* Initialize the `TableClient` object: Create a new instance of the `TableClient` that you have chosen in [Step 2](#build-your-own TableClient). 
* Initialize the Kernel objects and get the schema: Assuming the query is on the latest available version/snapshot of the table, you can get the table schema as follows:

```java
import io.delta.kernel.*;
import io.delta.kernel.defaults.clients.*';

TableClient myTableClient = new MyTableClient();
Table myTable = Table.forPath(myTablePath);
Snapshot mySnapshot = myTable.getLatestSnapshot(myTableClient);
StructType mySchema = mySnapshot.getSchema(myTableClient);
```

If you want to query a specific version of the table (that is, not the schema), then you can get the required snapshot as `myTable.getSnapshot(version)`.

#### Step 3.2: Resolve files to scan

Next, we need to build a Scan object using the more information from the query. Here we are going to assume that the connector/engine has been able extract the following details from the query (say, after the optimizing the logical plan):

* Read schema: The columns in the table that query needs to read. This may be the full set of columns or a subset of columns.
* Query filters: The filters on partitions or data columns that can be used skip reading table data.

To provide these information to Kernel, you have to do the following:

* Convert the engine-specific schema and filter expressions to Kernel schema and expressions: For schema, you have to create a `StructType` object. For the filters, you have to create an `Expression` object using all the available subclasses of `Expression`. 
* Build the scan with the converted information: Build the scan as follows:

```java
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;

StructType readSchema = ... ;  // convert engine schema
Expression filterExpr = ... ;  // convert engine filter expression

Scan myScan = mySnapshot.buildScan(tableClient)
  .withFilter(myTableClient, filterExpr)
  .withReadSchema(myTableClient, readSchema)
  .build()

```

* Resolve the information required to file reads: The generated Scan object has two sets of information. 
  * Scan files: `myScan.getScanFiles()` returns an iterator of `ColumnarBatches` . Each batch in the iterator contains rows and each row has information about a single file that has been selected based on the query filter.
  * Scan state: `myScan.getScanState()` returns a `Row` that contains all the information that is common across all the files that need to be read.

```java
Row myScanStateRow = myScan.getScanState();
CloseableIterator<ColumnarBatch> myScanFilesAsBatches = myScan.getScanFiles();

while (myScanFilesAsBatches.hasNext()) {
  ColumnarBatch scanFileBatch = myScanFilesAsBatches.next();

  CloseableIterator<Row> myScanFilesAsRows = scanFileBatch.getRows();  
}
```

As we will soon see, reading the columnar data from a selected file will need to use both, the scan state row, and a scan file row with the file information.

##### Requirements and guarantees
Here are the details you need to ensure when defining this scan.

* The provided `readSchema` must be the exact schema of the data that the engine will expect when executing the query. Any mismatch in the schema defined during this query planning and the query execution will result in runtime failures. Hence you must build the scan with the readSchema only after the engine has finalize the logical plan after any optimizations like column pruning.
* When applicable (for example, with Java Kernel APIs), you have to make sure to call the close() method as you consume the `ColumnarBatch`es of scan files (that is, either serialize the rows, or use them to read the table data).

#### Step 3.3: Distribute the file information to the workers
If you are building a connector for a distributed engine like Spark/Presto/Trino/Flink, then your connector has to send all the scan metadata from the query planning machine (henceforth called the driver) to task execution machines (henceforth called the workers). You will have to serialize and deserialize the scan state and scan file rows. It is the connector job to implement serialization and deserialization utilities for a [`Row`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/data/Row.html). 

##### Custom `Row` Serializer/Deserializer
Here are steps on how to build your own serializer/deserializer such that it will work with any [`Row`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/data/Row.html) of any schema. 

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

// Send these over to the worker

// In the worker when data will be read, after rowBytes have been sent over
Row scanStateRow = RowUtils.deserialize(scanStateRowBytes)
Row scanFileRow = RowUtils.deserialize(scanFileRowBytes)
```

#### Step 3.4: Read the columnar data 
Finally, we are ready to read the columnar data. You will have to do the following:

* Get the iterator of `ColumnarBatch`es: Use the scan state row and scan file row(s) to call the following to get a new iterator of data batches.

```java
Row myScanStateRow = ... ;
Iterator<Row> myScanFileRows = ... ;
CloseableIterator<ColummarBatchResult> dataIter = ...;

// Additional option predicate such as dynamic filters the connector wants to
// pass to the reader when reading files.
Expression optPredicate = ...;

CloseableIterator<DataReadResult> dataIter = Scan.getData(
  myTableClient,
  myScanStateRow,
  myScanFileRows,
  Optional<>.empty(optPredicate)
);

```

* Resolve the data in the batches: Each [`DataReadResult`](https://delta-io.github.io/delta/snapshot/kernel-api/java/api/io/delta/kernel/data/DataReadResult.html) has two components:
    * Columnar batch (returned by `DataReadResult.getData()`): This is the data read from the files having the schema matching the readSchema provided when the Scan object was built in the earlier step.
    * Optional selection vector (returned by `DataReadResult.getSelectionVector()`): Optionally, a boolean vector that will define which rows in the batch are valid and should be consumed by the engine.

If the selection vector is present, then you will have to apply it on the batch to resolve the final consumable data. 

* Convert to engine-specific data format: Each connector/engine has its own native row / columnar batch formats and interfaces. To return the read data batches to the engine, you have to convert them to fit those engine-specific formats and/or interfaces. Here are a few tips that you can follow to make this efficient.
  * Matching the engine-specific format: Some engines may expect the data in in-memory format that may be different from the data produced by `getData()`. So you will have to do the data conversion for each column vector in the batch as needed. 
  * Matching the engine-specific interfaces: You may have to implement wrapper classes that extend the engine-specific interfaces and appropriately encapsulate the row data.

For best performance, you can implement your own `ParquetHandler` and other interfaces to make sure that every `ColumnVector` generated is already in the engine-native format thus eliminating any need to convert. 

Now you should be able to read the Delta table correctly.
