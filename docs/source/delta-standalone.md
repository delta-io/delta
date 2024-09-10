---
description: Learn how to read and write Delta tables from JVM applications without <AS>.
---

# Delta Standalone (deprecated)

.. warning:: Delta Standalone is deprecated and will be removed in a future release. We recommend using the [Delta Kernel](delta-kernel.md) APIs.

The Delta Standalone library is a single-node Java library that can be used to read from and write to Delta tables. Specifically, this library provides APIs to interact with a table's metadata in the transaction log, implementing the [Delta Transaction Log Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) to achieve the transactional guarantees of the <Delta> format. Notably, this project doesn't depend on <AS> and has only a few transitive dependencies. Therefore, it can be used by any processing engine or application to access Delta tables.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
<!-- **this has been reformatted after generation via doctoc** -->

- [Use cases](#use-cases)
  - [Caveats](#caveats)
- [APIs](#apis)
  - [DeltaLog](#deltalog)
  - [Snapshot](#snapshot)
  - [OptimisticTransaction](#optimistictransaction)
  - [DeltaScan](#deltascan)
- [API compatibility](#api-compatibility)
- [Project setup](#project-setup)
  - [Environment requirements](#environment-requirements)
  - [Build files](#build-files)
    - [Maven](#maven)
    - [SBT](#sbt)
  - [Storage configuration](#storage-configuration)
    - [Amazon S3 configuration](#amazon-s3-configuration)
      - [Single-cluster setup (default)](#single-cluster-setup-default)
        - [Requirements (S3 single-cluster)](#requirements-s3-single-cluster)
        - [Configuration (S3 single-cluster)](#configuration-s3-single-cluster)
      - [Multi-cluster setup](#multi-cluster-setup)
        - [Requirements (S3 multi-cluster)](#requirements-s3-multi-cluster)
        - [Configuration (S3 multi-cluster)](#configuration-s3-multi-cluster)
        - [Production Configuration (S3 multi-cluster)](#production-configuration-s3-multi-cluster)
    - [Microsoft Azure configuration](#microsoft-azure-configuration)
      - [Azure Blob Storage](#azure-blob-storage)
        - [Requirements (Azure Blob storage)](#requirements-azure-blob-storage)
        - [Configuration (Azure Blob storage)](#configuration-azure-blob-storage)
      - [Azure Data Lake Storage Gen1](#azure-data-lake-storage-gen1)
        - [Requirements (ADLS Gen 1)](#requirements-adls-gen-1)
        - [Configuration (ADLS Gen 1)](#configuration-adls-gen-1)
      - [Azure Data Lake Storage Gen2](#azure-data-lake-storage-gen2)
        - [Requirements (ADLS Gen 2)](#requirements-adls-gen-2)
        - [Configuration (ADLS Gen 2)](#configuration-adls-gen-2)
    - [HDFS](#hdfs)
    - [Google Cloud Storage](#google-cloud-storage)
      - [Requirements (GCS)](#requirements-gcs)
      - [Configuration (GCS)](#configuration-gcs)
- [Usage](#usage)
  - [1. SBT configuration](#1-sbt-configuration)
  - [2. Mock situation](#2-mock-situation)
  - [3. Starting a transaction and finding relevant files](#3-starting-a-transaction-and-finding-relevant-files)
  - [4. Writing updated Parquet data](#4-writing-updated-parquet-data)
  - [5. Committing to our Delta table](#5-committing-to-our-delta-table)
  - [6. Reading from the Delta table](#6-reading-from-the-delta-table)
    - [6.1. Reading Parquet data (distributed)](#61-reading-parquet-data-distributed)
    - [6.2. Reading Parquet data (single-JVM)](#62-reading-parquet-data-single-jvm)
- [Reporting issues](#reporting-issues)
- [Contributing](#contributing)
- [Community](#community)
- [Local development](#local-development)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Use cases

Delta Standalone is optimized for cases when you want to read and write Delta tables by using a non-Spark engine of your choice. It is a "low-level" library, and we encourage developers to contribute open-source, higher-level connectors for their desired engines that use Delta Standalone for all Delta Lake metadata interaction. You can find a Hive source connector and Flink sink/source connector in the [Delta Lake](https://github.com/delta-io/delta) repository. Additional connectors are in development.

### Caveats

Delta Standalone minimizes memory usage in the JVM by loading the Delta Lake transaction log incrementally, using an iterator. However, Delta Standalone runs in a single JVM, and is limited to the processing and memory capabilities of that JVM. Users must configure the JVM to avoid out of memory (OOM) issues.

Delta Standalone does provide basic APIs for reading Parquet data, but does not include APIs for writing Parquet data. Users must write out new Parquet data files themselves and then use Delta Standalone to commit those changes to the Delta table and make the new data visible to readers.

## APIs

Delta Standalone provides classes and entities to read data, query metadata, and commit to the transaction log. A few of them are highlighted here and with their key interfaces. See the [Java API docs](https://delta-io.github.io/connectors/latest/delta-standalone/api/java/index.html) for the full set of classes and entities.

### DeltaLog

[DeltaLog](https://delta-io.github.io/connectors/latest/delta-standalone/api/java/io/delta/standalone/DeltaLog.html) is the main interface for programmatically interacting with the metadata in the transaction log of a Delta table.

- Instantiate a `DeltaLog` with `DeltaLog.forTable(hadoopConf, path)` and pass in the `path` of the root location of the Delta table.
- Access the current snapshot with `DeltaLog::snapshot`.
- Get the latest snapshot, including any new data files that were added to the log, with `DeltaLog::update`.
- Get the snapshot at some historical state of the log with `DeltaLog::getSnapshotForTimestampAsOf` or `DeltaLog::getSnapshotForVersionAsOf`.
- Start a new transaction to commit to the transaction log by using `DeltaLog::startTransaction`.
- Get all metadata actions without computing a full Snapshot using `DeltaLog::getChanges`.

### Snapshot

A [Snapshot](https://delta-io.github.io/connectors/latest/delta-standalone/api/java/io/delta/standalone/Snapshot.html) represents the state of the table at a specific version.

- Get a list of the metadata files by using `Snapshot::getAllFiles`.
- For a memory-optimized iterator over the metadata files, use `Snapshot::scan` to get a `DeltaScan` (as described later), optionally by passing in a `predicate` for partition filtering.
- Read actual data with `Snapshot::open`, which returns an iterator over the rows of the Delta table.

### OptimisticTransaction

The main class for committing a set of updates to the transaction log is [OptimisticTransaction](https://delta-io.github.io/connectors/latest/delta-standalone/api/java/io/delta/standalone/OptimisticTransaction.html). During a transaction, all reads must go through the `OptimisticTransaction` instance rather than the `DeltaLog` in order to detect logical conflicts and concurrent updates.

- Read metadata files during a transaction with `OptimisticTransaction::markFilesAsRead`, which returns a `DeltaScan` of files that match the `readPredicate`.
- Commit to the transaction log with `OptimisticTransaction::commit`.
- Get the latest version committed for a given application ID (for example, for idempotency) with `OptimisticTransaction::txnVersion`. (Note that this API requires users to commit `SetTransaction` actions.)
- Update the medadata of the table upon committing with `OptimisticTransaction::updateMetadata`.

### DeltaScan

[DeltaScan](https://delta-io.github.io/connectors/latest/delta-standalone/api/java/io/delta/standalone/DeltaScan.html) is a wrapper class for the files inside a `Snapshot` that match a given `readPredicate`.

- Access the files that match the partition filter portion of the `readPredicate` with `DeltaScan::getFiles`. This returns a memory-optimized iterator over the metadata files in the table.
- To further filter the returned files on non-partition columns, get the portion of input predicate not applied with `DeltaScan::getResidualPredicate`.

## API compatibility

The only public APIs currently provided by Delta Standalone are in the `io.delta.standalone` package. Classes and methods in the `io.delta.standalone.internal` package are considered internal and are subject to change across minor and patch releases.

## Project setup

You can add the Delta Standalone library as a dependency by using your preferred build tool. Delta Standalone depends upon the `hadoop-client` and `parquet-hadoop` packages. Example build files are listed in the following sections.

### Environment requirements

- JDK 8 or above.
- Scala 2.11 or 2.12.

### Build files

#### Maven

Replace the version of `hadoop-client` with the one you are using.

Scala 2.12:

```xml
<dependency>
  <groupId>io.delta</groupId>
  <artifactId>delta-standalone_2.12</artifactId>
  <version>0.5.0</version>
</dependency>
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-client</artifactId>
  <version>3.1.0</version>
</dependency>
```

Scala 2.11:

```xml
<dependency>
  <groupId>io.delta</groupId>
  <artifactId>delta-standalone_2.11</artifactId>
  <version>0.5.0</version>
</dependency>
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-client</artifactId>
  <version>3.1.0</version>
</dependency>
```

#### SBT

Replace the version of `hadoop-client` with the one you are using.

```
libraryDependencies ++= Seq(
  "io.delta" %% "delta-standalone" % "0.5.0",
  "org.apache.hadoop" % "hadoop-client" % "3.1.0)
```

#### `ParquetSchemaConverter` caveat

Delta Standalone shades its own Parquet dependencies so that it works out-of-the-box and reduces dependency conflicts in your environment. However, if you would like to use utility class `io.delta.standalone.util.ParquetSchemaConverter`, then you must provide your own version of `org.apache.parquet:parquet-hadoop`.

### Storage configuration

Delta Lake ACID guarantees are based on the atomicity and durability guarantees of the storage system. Not all storage systems provide all the necessary guarantees.

Because storage systems do not necessarily provide all of these guarantees out-of-the-box, <Delta> transactional operations typically go through the [LogStore API](https://github.com/delta-io/delta/blob/master/storage/src/main/java/io/delta/storage/LogStore.java) instead of accessing the storage system directly. To provide the ACID guarantees for different storage systems, you may have to use different `LogStore` implementations. This section covers how to configure Delta Standalone for various storage systems. There are two categories of storage systems:

- **Storage systems with built-in support**: For some storage systems, you do not need additional configurations. Delta Standalone uses the scheme of the path (that is, `s3a` in `s3a://path`) to dynamically identify the storage system and use the corresponding `LogStore` implementation that provides the transactional guarantees. However, for S3, there are additional caveats on concurrent writes. See the [section on S3](#amazon-s3-configuration) for details.

- **Other storage systems**: The `LogStore`, similar to Apache Spark, uses the Hadoop `FileSystem` API to perform reads and writes. Delta Standalone supports concurrent reads on any storage system that provides an implementation of the `FileSystem` API. For concurrent writes with transactional guarantees, there are two cases based on the guarantees provided by the `FileSystem` implementation. If the implementation provides consistent listing and atomic renames-without-overwrite (that is, `rename(... , overwrite = false)` will either generate the target file atomically or fail if it already exists with `java.nio.file.FileAlreadyExistsException`), then the default `LogStore` implementation using renames will allow concurrent writes with guarantees. Otherwise, you must configure a custom implementation of `LogStore` by setting the following Hadoop configuration when you instantiate a `DeltaLog` with `DeltaLog.forTable(hadoopConf, path)`:

  ```java
  delta.logStore.<scheme>.impl=<full-qualified-class-name>
  ```

  Here, `<scheme>` is the scheme of the paths of your storage system. This configures Delta Standalone to dynamically use the given `LogStore` implementation only for those paths. You can have multiple such configurations for different schemes in your application, thus allowing it to simultaneously read and write from different storage systems.

  .. note::
    - Before version 0.5.0, Delta Standalone supported configuring LogStores by setting `io.delta.standalone.LOG_STORE_CLASS_KEY`. This approach is now deprecated. Setting this configuration will use the configured `LogStore` for all paths, thereby disabling the dynamic scheme-based delegation.

#### Amazon S3 configuration

Delta Standalone supports reads and writes to S3 in two different modes: Single-cluster and Multi-cluster.

.. csv-table::
  :header: "", "Single-cluster", "Multi-cluster"

  Configuration,Comes out-of-the-box,Is experimental and requires extra configuration
  Reads,Supports concurrent reads from multiple clusters,Supports concurrent reads from multiple clusters
  Writes,Supports concurrent writes from a single cluster,Supports multi-cluster writes
  Permissions,S3 credentials,S3 and DynamoDB operating permissions

##### Single-cluster setup (default)

By default, Delta Standalone supports concurrent reads from multiple clusters. However, concurrent writes to S3 must originate from a single cluster to provide transactional guarantees. This is because S3 currently does not provide mutual exclusion, that is, there is no way to ensure that only one writer is able to create a file. 

.. warning:: Concurrent writes to the same Delta table from multiple Spark drivers can lead to data loss.

To use Delta Standalone with S3, you must meet the following requirements. If you are using access keys for authentication and authorization, you must configure a Hadoop Configuration specified as follows when you instantiate a `DeltaLog` with `DeltaLog.forTable(hadoopConf, path)`.

###### Requirements (S3 single-cluster)

- S3 credentials: [IAM roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) (recommended) or access keys.
- Hadoop's [AWS connector (hadoop-aws)](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws) for the version of Hadoop that Delta Standalone is compiled with.

###### Configuration (S3 single-cluster)

1. Include `hadoop-aws` JAR in the classpath.
2. Set up S3 credentials. We recommend that you use [IAM roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) for authentication and authorization. But if you want to use keys, configure your `org.apache.hadoop.conf.Configuration` with:

    ```java
    conf.set("fs.s3a.access.key", "<your-s3-access-key>");
    conf.set("fs.s3a.secret.key", "<your-s3-secret-key>");
    ```

##### Multi-cluster setup

.. note::
  This support is new and experimental.

  This mode supports concurrent writes to S3 from multiple clusters. Enable multi-cluster support by configuring Delta Standalone to use the correct `LogStore` implementation. This implementation uses [DynamoDB](https://aws.amazon.com/dynamodb/) to provide mutual exclusion.

.. warning::
  When writing from multiple clusters, all drivers must use this `LogStore` implementation and the same DynamoDB table and region. If some drivers use the default `LogStore` while others use this experimental `LogStore` then data loss can occur.

###### Requirements (S3 multi-cluster)
- All of the requirements listed in the [_](#requirements-s3-single-cluster) section
- In additon to S3 credentials, you also need DynamoDB operating permissions

###### Configuration (S3 multi-cluster)

#. Create the DynamoDB table. See [Create the DynamoDB table](delta-storage.md#setup-configuration-s3-multi-cluster) for more details on creating a table yourself (recommended)  or having it created for you automatically.

#. Follow the configuration steps listed in [_](#configuration-s3-single-cluster) section.

#. Include the `delta-storage-s3-dynamodb` JAR in the classpath.

#. Configure the `LogStore` implementation.

   First, configure this `LogStore` implementation for the scheme `s3`. You can replicate this command for schemes `s3a` and `s3n` as well.

   ```java
   conf.set("delta.logStore.s3.impl", "io.delta.storage.S3DynamoDBLogStore");
   ```

   .. csv-table::
     :header: "Configuration Key", "Description", "Default"

     io.delta.storage.S3DynamoDBLogStore.ddb.tableName, The name of the DynamoDB table to use, delta_log
     io.delta.storage.S3DynamoDBLogStore.ddb.region, The region to be used by the client, us-east-1
     io.delta.storage.S3DynamoDBLogStore.credentials.provider, The AWSCredentialsProvider* used by the client, [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html)
     io.delta.storage.S3DynamoDBLogStore.provisionedThroughput.rcu, (Table-creation-only**) Read Capacity Units, 5
     io.delta.storage.S3DynamoDBLogStore.provisionedThroughput.wcu, (Table-creation-only**) Write Capacity Units, 5

   - *For more details on AWS credential providers, see the [AWS documentation](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html).
   - **These configurations are only used when the given DynamoDB table doesn't already exist and needs to be automatically created.

###### Production Configuration (S3 multi-cluster)

By this point, this multi-cluster setup is fully operational. However, there is extra configuration you may do to improve performance and optimize storage when running in production. See the [Delta Lake documentation](delta-storage.md#production-configuration-s3-multi-cluster) for more details.

#### Microsoft Azure configuration

Delta Standalone supports concurrent reads and writes from multiple clusters with full transactional guarantees for various Azure storage systems. To use an Azure storage system, you must satisfy the following requirements, and configure a Hadoop Configuration as specified when you instantiate a `DeltaLog` with `DeltaLog.forTable(hadoopConf, path)`.

##### Azure Blob Storage

###### Requirements (Azure Blob storage)

- A [shared key](https://docs.microsoft.com/rest/api/storageservices/authorize-with-shared-key) or [shared access signature (SAS)](https://docs.microsoft.com/azure/storage/common/storage-sas-overview).
- Hadoop’s [Azure Blob Storage libraries](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure) for a version compatible with the Hadoop version Delta Standalone was compiled with.

  - 2.9.1+ for Hadoop 2
  - 3.0.1+ for Hadoop 3

###### Configuration (Azure Blob storage)

1. Include `hadoop-azure` JAR in the classpath.
2. Set up credentials.

    - For an SAS token, configure `org.apache.hadoop.conf.Configuration`:

      ```java
      conf.set(
        "fs.azure.sas.<your-container-name>.<your-storage-account-name>.blob.core.windows.net",
        "<complete-query-string-of-your-sas-for-the-container>");
      ```

    - To specify an account access key:

      ```java
      conf.set(
        "fs.azure.account.key.<your-storage-account-name>.blob.core.windows.net",
        "<your-storage-account-access-key>");
      ```

##### Azure Data Lake Storage Gen1

###### Requirements (ADLS Gen 1)

- A [service principal](https://docs.microsoft.com/azure/active-directory/develop/app-objects-and-service-principals) for OAuth 2.0 access.
- Hadoop's [Azure Data Lake Storage Gen1 libraries](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure-datalake) for a version that is compatible with the Hadoop version that was used to compile Delta Standalone.

    - 2.9.1+ for Hadoop 2
    - 3.0.1+ for Hadoop 3

###### Configuration (ADLS Gen 1)

1. Include `hadoop-azure-datalake` JAR in the classpath.
2. Set up Azure Data Lake Storage Gen1 credentials. Configure `org.apache.hadoop.conf.Configuration`:

    ```java
    conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential");
    conf.set("dfs.adls.oauth2.client.id", "<your-oauth2-client-id>");
    conf.set("dfs.adls.oauth2.credential", "<your-oauth2-credential>");
    conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/<your-directory-id>/oauth2/token");
    ```

##### Azure Data Lake Storage Gen2

###### Requirements (ADLS Gen 2)

- Account created in [Azure Data Lake Storage Gen2](https://docs.microsoft.com/azure/storage/blobs/create-data-lake-storage-account).
- Service principal [created](https://docs.microsoft.com/azure/active-directory/develop/howto-create-service-principal-portal) and [assigned the Storage Blob Data Contributor role](https://docs.microsoft.com/azure/storage/blobs/assign-azure-role-data-access) for the storage account.

    - Make a note of the storage-account-name, directory-id (also known as tenant-id), application-id, and password of the principal. These will be used for configuration.

- Hadoop's [Azure Data Lake Storage Gen2 libraries](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure-datalake) version 3.2+ and Delta Standalone compiled with Hadoop 3.2+.

###### Configuration (ADLS Gen 2)

1. Include `hadoop-azure-datalake` JAR in the classpath. In addition, you may also have to include JARs for Maven artifacts `hadoop-azure` and `wildfly-openssl`.
2. Set up Azure Data Lake Storage Gen2 credentials. Configure your `org.apache.hadoop.conf.Configuration` with:

    ```java
    conf.set("fs.azure.account.auth.type.<storage-account-name>.dfs.core.windows.net", "OAuth");
    conf.set("fs.azure.account.oauth.provider.type.<storage-account-name>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
    conf.set("fs.azure.account.oauth2.client.id.<storage-account-name>.dfs.core.windows.net", "<application-id>");
    conf.set("fs.azure.account.oauth2.client.secret.<storage-account-name>.dfs.core.windows.net","<password>");
    conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account-name>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token");
    ```

    where `<storage-account-name>`, `<application-id>`, `<directory-id>` and `<password>` are details of the service principal we set as requirements earlier.

#### HDFS

Delta Standalone has built-in support for HDFS with full transactional guarantees on concurrent reads and writes from multiple clusters. See [Hadoop documentation](https://hadoop.apache.org/docs/stable/) for configuring credentials.

#### Google Cloud Storage

##### Requirements (GCS)

- JAR of the [GCS Connector (gcs-connector)](https://search.maven.org/search?q=a:gcs-connector) Maven artifact.
- Google Cloud Storage account and credentials

##### Configuration (GCS)

1. Include the JAR for `gcs-connector` in the classpath. See the [documentation](https://cloud.google.com/dataproc/docs/tutorials/gcs-connector-spark-tutorial) for details on how to configure your project with the GCS connector.

## Usage

This example shows how to use Delta Standalone to:
- Find parquet files.
- Write parquet data.
- Commit to the transaction log.
- Read from the transaction log.
- Read back the Parquet data.

Please note that this example uses a fictitious, non-Spark engine `Zappy` to write the actual parquet data, as Delta Standalone does not provide any data-writing APIs. Instead, Delta Standalone Writer lets you commit metadata to the Delta log after you've written your data. This is why Delta Standalone works well with so many connectors (e.g. Flink, Presto, Trino, etc.) since they provide the parquet-writing functionality instead.

### 1. SBT configuration

The following SBT project configuration is used:

```scala
// <project-root>/build.sbt

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "io.delta" %% "delta-standalone" % "0.5.0",
  "org.apache.hadoop" % "hadoop-client" % "3.1.0")
```

### 2. Mock situation

We have a Delta table `Sales` storing sales data, but have realized all the data written on November 2021 for customer `XYZ` had incorrect `total_cost` values. Thus, we need to update all those records with the correct values. We will use a fictious distributed engine `Zappy` and Delta Standalone to update our Delta table.

The sales table schema is given below.

```
Sales
 |-- year: int          // partition column
 |-- month: int         // partition column
 |-- day: int           // partition column
 |-- customer: string
 |-- sale_id: string
 |-- total_cost: float
```

### 3. Starting a transaction and finding relevant files

Since we must read existing data in order to perform the desired update operation, we must use `OptimisticTransaction::markFilesAsRead` in order to automatically detect any concurrent modifications made to our read partitions. Since Delta Standalone only supports partition pruning, we must apply the residual predicate to further filter the returned files.

```java
import io.delta.standalone.DeltaLog;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.expressions.And;
import io.delta.standalone.expressions.EqualTo;
import io.delta.standalone.expressions.Literal;

DeltaLog log = DeltaLog.forTable(new Configuration(), "/data/sales");
OptimisticTransaction txn = log.startTransaction();

DeltaScan scan = txn.markFilesAsRead(
    new And(
        new And(
            new EqualTo(schema.column("year"), Literal.of(2021)),  // partition filter
            new EqualTo(schema.column("month"), Literal.of(11))),  // partition filter
        new EqualTo(schema.column("customer"), Literal.of("XYZ"))  // non-partition filter
    )
);

CloseableIterator<AddFile> iter = scan.getFiles();
Map<String, AddFile> addFileMap = new HashMap<String, AddFile>();  // partition filtered files: year=2021, month=11
while (iter.hasNext()) {
    AddFile addFile = iter.next();
    addFileMap.put(addFile.getPath(), addFile);
}
iter.close();

List<String> filteredFiles = ZappyReader.filterFiles( // fully filtered files: year=2021, month=11, customer=XYZ
    addFileMap.keySet(),
    toZappyExpression(scan.getResidualPredicate())
);
```

### 4. Writing updated Parquet data

Since Delta Standalone does not provide any Parquet data write APIs, we use `Zappy` to write the data.

```java
ZappyDataFrame correctedSaleIdToTotalCost = ...;

ZappyDataFrame invalidSales = ZappyReader.readParquet(filteredFiles);
ZappyDataFrame correctedSales = invalidSales.join(correctedSaleIdToTotalCost, "id");

ZappyWriteResult dataWriteResult = ZappyWritter.writeParquet("/data/sales", correctedSales);
```

The written data files from the preceding code will have a hierarchy similar to the following:

```shell
$ tree /data/sales
.
├── _delta_log
│   └── ...
│   └── 00000000000000001082.json
│   └── 00000000000000001083.json
├── year=2019
│   └── month=1
...
├── year=2020
│   └── month=1
│       └── day=1
│           └── part-00000-195768ae-bad8-4c53-b0c2-e900e0f3eaee-c000.snappy.parquet // previous
│           └── part-00001-53c3c553-f74b-4384-b9b5-7aa45bc2291b-c000.snappy.parquet // new
|           ...
│       └── day=2
│           └── part-00000-b9afbcf5-b90d-4f92-97fd-a2522aa2d4f6-c000.snappy.parquet // previous
│           └── part-00001-c0569730-5008-42fa-b6cb-5a152c133fde-c000.snappy.parquet // new
|           ...
```

### 5. Committing to our Delta table

Now that we've written the correct data, we need to commit to the transaction log to add the new files, and remove the old incorrect files.

```java
import io.delta.standalone.Operation;
import io.delta.standalone.actions.RemoveFile;
import io.delta.standalone.exceptions.DeltaConcurrentModificationException;
import io.delta.standalone.types.StructType;

List<RemoveFile> removeOldFiles = filteredFiles.stream()
    .map(path -> addFileMap.get(path).remove())
    .collect(Collectors.toList());

List<AddFile> addNewFiles = dataWriteResult.getNewFiles()
    .map(file ->
        new AddFile(
            file.getPath(),
            file.getPartitionValues(),
            file.getSize(),
            System.currentTimeMillis(),
            true, // isDataChange
            null, // stats
            null  // tags
        );
    ).collect(Collectors.toList());

List<Action> totalCommitFiles = new ArrayList<>();
totalCommitFiles.addAll(removeOldFiles);
totalCommitFiles.addAll(addNewFiles);

try {
    txn.commit(totalCommitFiles, new Operation(Operation.Name.UPDATE), "Zippy/1.0.0");
} catch (DeltaConcurrentModificationException e) {
    // handle exception here
}
```

### 6. Reading from the Delta table

Delta Standalone provides APIs that read both metadata and data, as follows.

#### 6.1. Reading Parquet data (distributed)

For most use cases, and especially when you deal with large volumes of data, we recommend that you use the Delta Standalone library as your metadata-only reader, and then perform the Parquet data reading yourself, most likely in a distributed manner.

Delta Standalone provides two APIs for reading the files in a given table snapshot. `Snapshot::getAllFiles` returns an in-memory list. As of 0.3.0, we also provide `Snapshot::scan(filter)::getFiles`, which supports partition pruning and an optimized internal iterator implementation. We will use the latter here.

```java
import io.delta.standalone.Snapshot;

DeltaLog log = DeltaLog.forTable(new Configuration(), "/data/sales");
Snapshot latestSnapshot = log.update();
StructType schema = latestSnapshot.getMetadata().getSchema();
DeltaScan scan = latestSnapshot.scan(
    new And(
        new And(
            new EqualTo(schema.column("year"), Literal.of(2021)),
            new EqualTo(schema.column("month"), Literal.of(11))),
        new EqualTo(schema.column("customer"), Literal.of("XYZ"))
    )
);

CloseableIterator<AddFile> iter = scan.getFiles();

try {
    while (iter.hasNext()) {
        AddFile addFile = iter.next();

        // Zappy engine to handle reading data in `addFile.getPath()` and apply any `scan.getResidualPredicate()`
    }
} finally {
    iter.close();
}
```

#### 6.2. Reading Parquet data (single-JVM)

Delta Standalone allows reading the Parquet data directly, using `Snapshot::open`.

```java
import io.delta.standalone.data.RowRecord;

CloseableIterator<RowRecord> dataIter = log.update().open();

try {
    while (dataIter.hasNext()) {
        RowRecord row = dataIter.next();
        int year = row.getInt("year");
        String customer = row.getString("customer");
        float totalCost = row.getFloat("total_cost");
    }
} finally {
    dataIter.close();
}
```

## Reporting issues
We use [GitHub Issues](https://github.com/delta-io/connectors/issues) to track community reported issues. You can also [contact](#community) the community for getting answers.

## Contributing

<!--- Update this to point to the website contributing guidelines when we have new website -->
We welcome contributions to Delta Lake repository. We use [GitHub Pull Requests](https://github.com/delta-io/delta/pulls) for accepting changes.

## Community

There are two ways to communicate with the Delta Lake community.
- Public Slack Channel
  - [Register to join the Slack channel](https://join.slack.com/t/delta-users/shared_invite/enQtNTY1NDg0ODcxOTI1LWJkZGU3ZmQ3MjkzNmY2ZDM0NjNlYjE4MWIzYjg2OWM1OTBmMWIxZTllMjg3ZmJkNjIwZmE1ZTZkMmQ0OTk5ZjA)
  - [Sign in to the Slack channel](https://delta-users.slack.com/)
- Public [mailing list](https://groups.google.com/forum/#!forum/delta-users)

## Local development

<!--- Update this with a more detailed guide? -->
- Before local debugging of `standalone` tests in IntelliJ, run all tests with `build/sbt standalone/test`. This helps IntelliJ recognize the golden tables as class resources.


.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark