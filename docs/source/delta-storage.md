---
description: Learn how to configure <Delta> on different storage systems.
---

# Storage configuration

<Delta> ACID guarantees are predicated on the atomicity and durability guarantees of the storage system. Specifically, <Delta> relies on the following when interacting with storage systems:

- **Atomic visibility**: There must a way for a file to visible in its entirety or not visible at all.

- **Mutual exclusion**: Only one writer must be able to create (or rename) a file at the final destination.

- **Consistent listing**: Once a file has been written in a directory, all future listings for that directory must return that file.

Because storage systems do not necessarily provide all of these guarantees out-of-the-box, <Delta> transactional operations typically go through the [LogStore API](https://github.com/delta-io/delta/blob/master/storage/src/main/java/io/delta/storage/LogStore.java) instead of accessing the storage system directly. To provide the ACID guarantees for different storage systems, you may have to use different `LogStore` implementations. This article covers how to configure <Delta> for various storage systems. There are two categories of storage systems:

- **Storage systems with built-in support**: For some storage systems, you do not need additional configurations. <Delta> uses the scheme of the path (that is, `s3a` in `s3a://path`) to dynamically identify the storage system and use the corresponding `LogStore` implementation that provides the transactional guarantees. However, for S3, there are additional caveats on concurrent writes. See the [section on S3](#delta-storage-s3) for details.

- **Other storage systems**: The `LogStore`, similar to Apache Spark, uses Hadoop `FileSystem` API to perform reads and writes. So <Delta> supports concurrent reads on any storage system that provides an implementation of `FileSystem` API. For concurrent writes with transactional guarantees, there are two cases based on the guarantees provided by `FileSystem` implementation. If the implementation provides consistent listing and atomic renames-without-overwrite (that is, `rename(... , overwrite = false)` will either generate the target file atomically or fail if it already exists with `java.nio.file.FileAlreadyExistsException`), then the default `LogStore` implementation using renames will allow concurrent writes with guarantees. Otherwise, you must configure a custom implementation of `LogStore` by setting the following Spark configuration

  ```ini
  spark.delta.logStore.<scheme>.impl=<full-qualified-class-name>
  ```

  where `<scheme>` is the scheme of the paths of your storage system. This configures <Delta> to dynamically use the given `LogStore` implementation only for those paths. You can have multiple such configurations for different schemes in your application, thus allowing it to simultaneously read and write from different storage systems.

  .. note::
    - <Delta> on local file system may not support concurrent transactional writes. This is because the local file system may or may not provide atomic renames. So you should not use the local file system for testing concurrent writes.
    - Before version 1.0, <Delta> supported configuring LogStores by setting `spark.delta.logStore.class`. This approach is now deprecated. Setting this configuration will use the configured `LogStore` for all paths, thereby disabling the dynamic scheme-based delegation.

.. contents:: In this article:
  :local:
  :depth: 1

<a id="delta-storage-s3"></a>
## Amazon S3

<Delta> supports reads and writes to S3 in two different modes: Single-cluster and Multi-cluster.

.. csv-table::
  :header: "", "Single-cluster", "Multi-cluster"

  Configuration,Comes with <Delta> out-of-the-box,Is experimental and requires extra configuration
  Reads,Supports concurrent reads from multiple clusters,Supports concurrent reads from multiple clusters
  Writes,Supports concurrent writes from a _single_ Spark driver,Supports multi-cluster writes
  Permissions,S3 credentials,S3 and DynamoDB\ScyllaDB operating permissions

<a id="delta-storage-s3-single-cluster"></a>
### Single-cluster setup (default)

In this default mode, <Delta> supports concurrent reads from multiple clusters, but concurrent writes to S3 must originate from a _single_ Spark driver in order for <Delta> to provide transactional guarantees. This is because S3 currently does not provide mutual exclusion, that is, there is no way to ensure that only one writer is able to create a file.

.. warning:: Concurrent writes to the same Delta table on S3 storage from multiple Spark drivers can lead to data loss. For a multi-cluster solution, please see the [_](#delta-storage-s3-multi-cluster) section below.

.. contents:: In this section:
  :local:
  :depth: 1

#### Requirements (S3 single-cluster)

- S3 credentials: [IAM roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) (recommended) or access keys
- <AS> associated with the corresponding <Delta> version.
- Hadoop's [AWS connector (hadoop-aws)](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/) for the version of Hadoop that <AS> is compiled for.

#### Quickstart (S3 single-cluster)

This section explains how to quickly start reading and writing Delta tables on S3 using single-cluster mode. For a detailed explanation of the configuration, see [_](#setup-configuration-s3-multi-cluster).

   ```bash
   bin/spark-shell \
    --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \

    --conf spark.hadoop.fs.s3a.access.key=<your-s3-access-key> \
    --conf spark.hadoop.fs.s3a.secret.key=<your-s3-secret-key>
   ```

#. Try out some basic Delta table operations on S3 (in Scala):

   ```scala
   // Create a Delta table on S3:
   spark.range(5).write.format("delta").save("s3a://<your-s3-bucket>/<path-to-delta-table>")

   // Read a Delta table on S3:
   spark.read.format("delta").load("s3a://<your-s3-bucket>/<path-to-delta-table>").show()
   ```

For other languages and more examples of Delta table operations, see the [_](quick-start.md) page.

For efficient listing of <Delta> metadata files on S3, set the configuration `delta.enableFastS3AListFrom=true`. This performance optimization is in experimental support mode. It will only work on `S3A` filesystems and will not work on [Amazon's EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-file-systems.html) default filesystem `S3`.

  ```scala
  bin/spark-shell \
    --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.hadoop.fs.s3a.access.key=<your-s3-access-key> \
    --conf spark.hadoop.fs.s3a.secret.key=<your-s3-secret-key> \
    --conf "spark.hadoop.delta.enableFastS3AListFrom=true
  ```

#### Configuration (S3 single-cluster)

Here are the steps to configure <Delta> for S3.

#. Include `hadoop-aws` JAR in the classpath.

   <Delta> needs the `org.apache.hadoop.fs.s3a.S3AFileSystem` class from the `hadoop-aws` package, which implements Hadoop's `FileSystem` API for S3. Make sure the version of this package matches the Hadoop version with which Spark was built.

#. Set up S3 credentials.

   We recommend using [IAM roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) for authentication and authorization. But if you want to use keys, here is one way is to set up the [Hadoop configurations](https://spark.apache.org/docs/latest/configuration.html#custom-hadoophive-configuration) (in Scala):

   ```scala
   sc.hadoopConfiguration.set("fs.s3a.access.key", "<your-s3-access-key>")
   sc.hadoopConfiguration.set("fs.s3a.secret.key", "<your-s3-secret-key>")
   ```

<a id="delta-storage-s3-multi-cluster"></a>

### Multi-cluster setup

.. note::
  This support is new and experimental.

This mode supports concurrent writes to S3 from multiple clusters and has to be explicitly enabled by configuring <Delta> to use the right `LogStore` implementation. This implementation uses [DynamoDB](https://aws.amazon.com/dynamodb/) or [ScyllaDB Alternator](https://opensource.docs.scylladb.com/stable/using-scylla/alternator/) to provide the mutual exclusion that S3 is lacking.

.. warning::
  This multi-cluster writing solution is only safe when all writers use this `LogStore` implementation as well as the same DynamoDB\ScyllaDB table and region\endpoint. If some drivers use out-of-the-box <Delta> while others use this experimental `LogStore`, then data loss can occur.

.. contents:: In this section:
  :local:
  :depth: 1

#### Requirements (S3 multi-cluster)
- All of the requirements listed in [_](#requirements-s3-single-cluster) section
- In addition to S3 credentials, you also need DynamoDB\ScyllaDB operating permissions

#### Quickstart (S3 multi-cluster)

This section explains how to quickly start reading and writing Delta tables on S3 using multi-cluster mode.

#. Use the following command to launch a Spark shell with <Delta> and S3 support (assuming you use Spark 3.5.0 which is pre-built for Hadoop 3.3.4):

   **For DynamoDB:**
   ```bash
   bin/spark-shell \
    --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-storage-s3-dynamodb:3.1.0 \
    --conf spark.hadoop.fs.s3a.access.key=<your-s3-access-key> \
    --conf spark.hadoop.fs.s3a.secret.key=<your-s3-secret-key> \
    --conf spark.delta.logStore.s3a.impl=io.delta.storage.S3DynamoDBLogStore \
    --conf spark.io.delta.storage.S3DynamoDBLogStore.ddb.region=us-west-2
   ```

   **For ScyllaDB:**
   ```bash
     bin/spark-shell \
      --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-storage-s3-dynamodb:3.1.0 \
      --conf spark.hadoop.fs.s3a.access.key=<your-s3-access-key> \
      --conf spark.hadoop.fs.s3a.secret.key=<your-s3-secret-key> \
      --conf spark.delta.logStore.s3a.impl=io.delta.storage.S3ScyllaDBLogStore \
      --conf spark.io.delta.storage.S3ScyllaDBLogStore.ddb.endpoint=<your-scylladb-endpoint>
  ```
     
#. Try out some basic Delta table operations on S3 (in Scala):
   ```scala
   // Create a Delta table on S3:
   spark.range(5).write.format("delta").save("s3a://<your-s3-bucket>/<path-to-delta-table>")

   // Read a Delta table on S3:
   spark.read.format("delta").load("s3a://<your-s3-bucket>/<path-to-delta-table>").show()
   ```

#### Setup Configuration (S3 multi-cluster)

#. Create the DynamoDB\ScyllaDB table.

   You have the choice of creating the DynamoDB\ScyllaDB table yourself (recommended) or having it created for you automatically.

   - Creating the DynamoDB\ScyllaDB table yourself

      This DynamoDB table will maintain commit metadata for multiple Delta tables, and it is important that it is configured with the [Read/Write Capacity Mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html) (for example, on-demand or provisioned) that is right for your use cases. As such, we strongly recommend that you create your DynamoDB\ScyllaDB table yourself. The following example uses the AWS CLI. To learn more, see the [create-table](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/create-table.html) command reference.

      **For DynamoDB:**
      ```bash
      aws dynamodb create-table \
        --region us-east-1 \
        --table-name delta_log \
        --attribute-definitions AttributeName=tablePath,AttributeType=S \
                                AttributeName=fileName,AttributeType=S \
        --key-schema AttributeName=tablePath,KeyType=HASH \
                     AttributeName=fileName,KeyType=RANGE \
        --billing-mode PAY_PER_REQUEST
      ```
     Note: once you select a `table-name` and `region`, you will have to specify them in each Spark session in order for this multi-cluster mode to work correctly. See table below. 

     **For ScyllaDB:**
     ```bash
      aws dynamodb create-table \
        --endpoint-url <your-scylla-db-endpoint> \
        --table-name delta_log \
        --attribute-definitions AttributeName=tablePath,AttributeType=S \
                                AttributeName=fileName,AttributeType=S \
        --key-schema AttributeName=tablePath,KeyType=HASH \
                     AttributeName=fileName,KeyType=RANGE
      ```
     Note: once you select a `table-name` and `endpoint`, you will have to specify them in each Spark session in order for this multi-cluster mode to work correctly. See table below.

   - Automatic DynamoDB\ScyllaDB table creation

      Nonetheless, after specifying this `LogStore `implementation, if the default DynamoDB\ScyllaDB table does not already exist, then it will be created for you automatically. If you are using DynamoDB the default table supports 5 strongly consistent reads and 5 writes per second. You may change these default values using the table-creation-only configurations keys detailed in the table below.

#. Follow the configuration steps listed in [_](#configuration-s3-single-cluster) section.

#. Include the `delta-storage-s3-dynamodb` JAR in the classpath.

#. Configure the `LogStore` implementation in your Spark session.

   First, configure this `LogStore` implementation for the scheme `s3`. You can replicate this command for schemes `s3a` and `s3n` as well.
   **For DynamoDB:**
   ```ini
   spark.delta.logStore.s3.impl=io.delta.storage.S3DynamoDBLogStore
   ```
   **For ScyllaDB:**
   ```ini
   spark.delta.logStore.s3.impl=io.delta.storage.S3ScyllaDBLogStore
   ```
   Next, specify additional information necessary to instantiate the DynamoDB/ScyllaDB client. You must instantiate the DynamoDB\ScyllaDB client with the same `tableName` and `region` \ `endpoint` each Spark session for this multi-cluster mode to work correctly. A list of per-session configurations and their defaults is given below:
   **For DynamoDB:**
   .. csv-table::
     :header: "Configuration Key", "Description", "Default"

     spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName, The name of the DynamoDB table to use, delta_log
     spark.io.delta.storage.S3DynamoDBLogStore.ddb.region, The region to be used by the client, us-east-1
     spark.io.delta.storage.S3DynamoDBLogStore.credentials.provider, The AWSCredentialsProvider* used by the client, [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html)
     spark.io.delta.storage.S3DynamoDBLogStore.provisionedThroughput.rcu, (Table-creation-only**) Read Capacity Units, 5
     spark.io.delta.storage.S3DynamoDBLogStore.provisionedThroughput.wcu, (Table-creation-only**) Write Capacity Units, 5

   - *For more details on AWS credential providers, see the [AWS documentation](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html).
   - **These configurations are only used when the given DynamoDB table doesn't already exist and needs to be automatically created.

   **For ScyllaDB:**
   .. csv-table::
     :header: "Configuration Key", "Description", "Default"

     spark.io.delta.storage.S3ScyllaDBLogStore.ddb.tableName, The name of the ScyllaDB table to use, delta_log
     spark.io.delta.storage.S3ScyllaDBLogStore.ddb.endpoint, The endpoint of ScyllaDB,
     spark.io.delta.storage.S3ScyllaDBLogStore.credentials.provider, The AWSCredentialsProvider* used by the client, [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html)

  - *For more details on AWS credential providers, see the [AWS documentation](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html).




#### Production Configuration (S3 multi-cluster)

By this point, this multi-cluster setup is fully operational. However, there is extra configuration you may do to improve performance and optimize storage when running in production.

#. Adjust your Read and Write Capacity Mode.

   If you are using the default DynamoDB table created for you by this `LogStore` implementation, its default RCU and WCU might not be enough for your workloads. You can [adjust the provisioned throughput](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ProvisionedThroughput.html#ProvisionedThroughput.CapacityUnits.Modifying) or [update to On-Demand Mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.Basics.html#WorkingWithTables.Basics.UpdateTable).

#. Cleanup old DynamoDB\ScyllaDB entries using Time to Live (TTL).

   Once a DynamoDB\ScyllaDB metadata entry is marked as complete, and after sufficient time such that we can now rely on S3 alone to prevent accidental overwrites on its corresponding Delta file, it is safe to delete that entry from DynamoDB\ScyllaDB. The cheapest way to do this is using [DynamoDB's TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html) feature which is a free, automated means to delete items from your DynamoDB\ScyllaDB table.

   If you create the DynamoDB\ScyllaDB table automatically, the TTL is already enabled on the table. If you create it manually, you will need to enable TTL manually.

   Run the following command on your given DynamoDB\ScyllaDB table to enable TTL:

   **For DynamoDB:**
   ```bash
   aws dynamodb update-time-to-live \
     --region us-east-1 \
     --table-name delta_log \
     --time-to-live-specification "Enabled=true, AttributeName=expireTime"
   ```
   **For ScyllaDB:**
   ```bash
   aws dynamodb update-time-to-live \
     --endpoint-url <your-scylla-db-endpoint> \
     --table-name delta_log \
     --time-to-live-specification "Enabled=true, AttributeName=expireTime"
   ```

   The default `expireTime` will be one day after the DynamoDB\ScyllaDB entry was marked as completed.

#. Cleanup old AWS S3 temp files using S3 Lifecycle Expiration.

   In this `LogStore` implementation, a temp file is created containing a copy of the metadata to be committed into the Delta log. Once that commit to the Delta log is complete, and after the corresponding DynamoDB entry has been removed, it is safe to delete this temp file. In practice, only the latest temp file will ever be used during recovery of a failed commit.

   Here are two simple options for deleting these temp files.

   #. Delete manually using S3 CLI.

      This is the safest option. The following command will delete all but the latest temp file in your given `<bucket>` and `<table>`:

      ```bash
      aws s3 ls s3://<bucket>/<delta_table_path>/_delta_log/.tmp/ --recursive | awk 'NF>1{print $4}' | grep . | sort | head -n -1  | while read -r line ; do
          echo "Removing ${line}"
          aws s3 rm s3://<bucket>/<delta_table_path>/_delta_log/.tmp/${line}
      done
      ```

   #. Delete using an S3 Lifecycle Expiration Rule

      A more automated option is to use an [S3 Lifecycle Expiration rule](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html), with filter prefix pointing to the `<delta_table_path>/_delta_log/.tmp/` folder located in your table path, and an expiration value of 30 days.

      Note: It is important that you choose a sufficiently large expiration value. As stated above, the latest temp file will be used during recovery of a failed commit. If this temp file is deleted, then your DynamoDB table and S3 `<delta_table_path>/_delta_log/.tmp/` folder will be out of sync.

      There are a variety of ways to configuring a bucket lifecycle configuration, described in AWS docs [here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/how-to-set-lifecycle-configuration-intro.html).

      One way to do this is using S3's `put-bucket-lifecycle-configuration` command. See [S3 Lifecycle Configuration](https://docs.aws.amazon.com/cli/latest/reference/s3api/put-bucket-lifecycle-configuration.html) for details. An example rule and command invocation is given below:

      In a file referenced as `file://lifecycle.json`:

      ```json
      {
        "Rules":[
          {
            "ID":"expire_tmp_files",
            "Filter":{
              "Prefix":"path/to/table/_delta_log/.tmp/"
            },
            "Status":"Enabled",
            "Expiration":{
              "Days":30
            }
          }
        ]
      }
      ```

      ```bash
      aws s3api put-bucket-lifecycle-configuration \
        --bucket my-bucket \
        --lifecycle-configuration file://lifecycle.json
      ```

.. note::
  AWS S3 may have a limit on the number of rules per bucket. See [PutBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html) for details.

## Microsoft Azure storage

<Delta> has built-in support for the various Azure storage systems with full transactional guarantees for concurrent reads and writes from multiple clusters.

<Delta> relies on Hadoop `FileSystem` APIs to access Azure storage services. Specifically, <Delta> requires the implementation of `FileSystem.rename()` to be atomic, which is only supported in newer Hadoop versions ([Hadoop-15156](https://issues.apache.org/jira/browse/HADOOP-15156) and [Hadoop-15086](https://issues.apache.org/jira/browse/HADOOP-15086))). For this reason, you may need to build Spark with newer Hadoop versions and use them for deploying your application. See [Specifying the Hadoop Version and Enabling YARN](https://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn) for building Spark with a specific Hadoop version and [_](quick-start.md) for setting up Spark with <Delta>.

Here is a list of requirements specific to each type of Azure storage system:

.. contents::
  :local:
  :depth: 1

### Azure Blob storage

#### Requirements (Azure Blob storage)

- A [shared key](https://docs.microsoft.com/rest/api/storageservices/authorize-with-shared-key) or [shared access signature (SAS)](https://docs.microsoft.com/azure/storage/common/storage-dotnet-shared-access-signature-part-1)
- <Delta> 0.2.0 or above
- Hadoop's Azure Blob Storage libraries for deployment with the following versions:
  - 2.9.1+ for Hadoop 2
  - 3.0.1+ for Hadoop 3
- <AS> associated with the corresponding <Delta> version (see the Quick Start page of the relevant Delta version's documentation) and [compiled with Hadoop version](https://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn) that is compatible with the chosen Hadoop libraries.

For example, a possible combination that will work is Delta 0.7.0 or above, along with <AS> 3.0 compiled and deployed with Hadoop 3.2.

#### Configuration (Azure Blob storage)

Here are the steps to configure <Delta> on Azure Blob storage.

#. Include `hadoop-azure` JAR in the classpath. See the requirements above for version details.

#. Set up credentials.

   You can set up your credentials in the [Spark configuration property](https://spark.apache.org/docs/latest/configuration.html).

   We recommend that you use a SAS token. In Scala, you can use the following:

   ```scala
   spark.conf.set(
     "fs.azure.sas.<your-container-name>.<your-storage-account-name>.blob.core.windows.net",
      "<complete-query-string-of-your-sas-for-the-container>")
   ```

   Or you can specify an account access key:

   ```scala
   spark.conf.set(
     "fs.azure.account.key.<your-storage-account-name>.blob.core.windows.net",
      "<your-storage-account-access-key>")
   ```

#### Usage (Azure Blob storage)

```scala
spark.range(5).write.format("delta").save("wasbs://<your-container-name>@<your-storage-account-name>.blob.core.windows.net/<path-to-delta-table>")
spark.read.format("delta").load("wasbs://<your-container-name>@<your-storage-account-name>.blob.core.windows.net/<path-to-delta-table>").show()
```


### Azure Data Lake Storage Gen1

#### Requirements (ADLS Gen1)

- A [service principal](https://docs.microsoft.com/azure/active-directory/develop/app-objects-and-service-principals) for OAuth 2.0 access
- <Delta> 0.2.0 or above
- Hadoop's Azure Data Lake Storage Gen1 libraries for deployment with the following versions:
  - 2.9.1+ for Hadoop 2
  - 3.0.1+ for Hadoop 3
- <AS> associated with the corresponding <Delta> version (see the Quick Start page of the relevant Delta version's documentation) and [compiled with Hadoop version](https://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn) that is compatible with the chosen Hadoop libraries.

For example, a possible combination that will work is Delta 0.7.0 or above, along with <AS> 3.0 compiled and deployed with Hadoop 3.2.

#### Configuration (ADLS Gen1)

Here are the steps to configure <Delta> on Azure Data Lake Storage Gen1.

#. Include `hadoop-azure-datalake` JAR in the classpath. See the requirements above for version details.

#. Set up Azure Data Lake Storage Gen1 credentials.

   You can set the following [Hadoop configurations](https://spark.apache.org/docs/latest/configuration.html#custom-hadoophive-configuration) with your credentials (in Scala):

   ```scala
   spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
   spark.conf.set("dfs.adls.oauth2.client.id", "<your-oauth2-client-id>")
   spark.conf.set("dfs.adls.oauth2.credential", "<your-oauth2-credential>")
   spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/<your-directory-id>/oauth2/token")
   ```

#### Usage (ADLS Gen1)

```scala
spark.range(5).write.format("delta").save("adl://<your-adls-account>.azuredatalakestore.net/<path-to-delta-table>")

spark.read.format("delta").load("adl://<your-adls-account>.azuredatalakestore.net/<path-to-delta-table>").show()
```


### Azure Data Lake Storage Gen2

#### Requirements (ADLS Gen2)

- Account created in [Azure Data Lake Storage Gen2]((https://docs.microsoft.com/en-us/azure/storage/common/storage-account-create))
- Service principal [created](https://docs.microsoft.com/azure/azure-resource-manager/resource-group-create-service-principal-portal) and [assigned the Storage Blob Data Contributor role](https://docs.microsoft.com/azure/storage/common/storage-auth-aad-rbac-portal?toc=%2fazure%2fstorage%2fblobs%2ftoc.json) for the storage account.
  - Note the storage-account-name, directory-id (also known as tenant-id), application-id, and password of the principal. These will be used for configuring Spark.

- <Delta> 0.7.0 or above
- <AS> 3.0 or above
- <AS> used must be built with Hadoop 3.2 or above.

For example, a possible combination that will work is Delta 0.7.0 or above, along with <AS> 3.0 compiled and deployed with Hadoop 3.2.

#### Configuration (ADLS Gen2)

Here are the steps to configure <Delta> on Azure Data Lake Storage Gen1.

#. Include the JAR of the Maven artifact `hadoop-azure-datalake` in the classpath. See the [requirements](#azure-blob-storage) for version details. In addition, you may also have to include JARs for Maven artifacts `hadoop-azure` and `wildfly-openssl`.

#. Set up Azure Data Lake Storage Gen2 credentials.

   ```scala
   spark.conf.set("fs.azure.account.auth.type.<storage-account-name>.dfs.core.windows.net", "OAuth")
   spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account-name>.dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
   spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account-name>.dfs.core.windows.net", "<application-id>")
   spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account-name>.dfs.core.windows.net","<password>")
   spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account-name>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")
  ```
  where `<storage-account-name>`, `<application-id>`, `<directory-id>` and `<password>` are details of the service principal we set as requirements earlier.

#. Initialize the file system if needed

```scala
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
```

#### Usage (ADLS Gen2)

```scala
spark.range(5).write.format("delta").save("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-delta-table>")

spark.read.format("delta").load("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-delta-table>").show()
```
where `<container-name>` is the file system name under the container.


## HDFS

<Delta> has built-in support for HDFS with full transactional guarantees on concurrent reads and writes from multiple clusters. See Hadoop and Spark documentation for configuring credentials.

## Google Cloud Storage

You must configure <Delta> to use the correct `LogStore` for concurrently reading and writing from GCS.

### Requirements (GCS)

- For <Delta> 1.1 and below, you must explicitly include the JAR of the [Delta Contributions (delta-contribs)](https://search.maven.org/search?q=g:io.delta%20AND%20a:delta-contribs*) Maven artifact of the same version.
- JAR of the [GCS Connector (gcs-connector)](https://search.maven.org/search?q=a:gcs-connector) Maven artifact.
- Google Cloud Storage account and credentials

### Configuration (GCS)

#. For <Delta> 1.2.0 and below, you must explicitly configure the LogStore implementation for the scheme `gs`.

    ```ini
    spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore
    ```

#. Include the JAR for `gcs-connector` in the classpath. See the [documentation](https://cloud.google.com/dataproc/docs/tutorials/gcs-connector-spark-tutorial) for details on how to configure Spark with GCS.

### Usage (GCS)

```scala
spark.range(5, 10).write.format("delta").mode("append").save("gs://<bucket-name>/<path-to-delta-table>")

spark.read.format("delta").load("gs://<bucket-name>/<path-to-delta-table>").show()
```


## Oracle Cloud Infrastructure

.. note::
  This support is new and experimental.

You have to configure <Delta> to use the correct `LogStore` for concurrently reading and writing.

### Requirements (OCI)

- JAR of the [Delta Contributions (delta-contribs)](https://search.maven.org/search?q=g:io.delta%20AND%20a:delta-contribs*) Maven artifact.
- JAR of the [OCI HDFS Connector (oci-hdfs-connector)](https://search.maven.org/search?q=a:oci-hdfs-connector) Maven artifact.
- [OCI account and Object Storage Access Credentials](https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/hdfsconnector.htm).

### Configuration (OCI)

#. Configure LogStore implementation for the scheme `oci`.

    ```ini
    spark.delta.logStore.oci.impl=io.delta.storage.OracleCloudLogStore
    ```

#. Include the JARs for `delta-contribs` and `hadoop-oci-connector` in the classpath. See [Using the HDFS Connector with Spark](https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/hdfsconnectorspark.htm) for details on how to configure Spark with OCI.

#. Set the OCI Object Store credentials as explained in the [documentation](https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/hdfsconnector.htm).

### Usage (OCI)

```scala
spark.range(5).write.format("delta").save("oci://<ociBucket>@<ociNameSpace>/<path-to-delta-table>")

spark.read.format("delta").load("oci://<ociBucket>@<ociNameSpace>/<path-to-delta-table>").show()
```


## IBM Cloud Object Storage

.. note::
  This support is new and experimental.

You have to configure <Delta> to use the correct `LogStore` for concurrently reading and writing.

### Requirements (IBM)

- JAR of the [Delta Contributions (delta-contribs)](https://search.maven.org/search?q=g:io.delta%20AND%20a:delta-contribs*) Maven artifact.
- JAR of the [Stocator (Stocator)](https://search.maven.org/search?q=a:stocator) Maven artifact, or build one that uses the IBM SDK following the [Stocator README](https://github.com/CODAIT/stocator).
- [IBM COS credentials](https://cloud.ibm.com/docs/services/cloud-object-storage/iam?topic=cloud-object-storage-service-credentials): IAM or access keys


### Configuration (IBM)

#. Configure LogStore implementation for the scheme `cos`.

    ```ini
    spark.delta.logStore.cos.impl=io.delta.storage.IBMCOSLogStore
    ```

#. Include the JARs for `delta-contribs` and `Stocator` in the classpath.

#. Configure `Stocator` with atomic write support by setting the following properties in the Hadoop configuration.

    ```ini
    fs.stocator.scheme.list=cos
    fs.cos.impl=com.ibm.stocator.fs.ObjectStoreFileSystem
    fs.stocator.cos.impl=com.ibm.stocator.fs.cos.COSAPIClient
    fs.stocator.cos.scheme=cos
    fs.cos.atomic.write=true
    ```

#. Set up IBM COS credentials. The example below uses access keys with a service named `service` (in Scala):

    ```
    sc.hadoopConfiguration.set("fs.cos.service.endpoint", "<your-cos-endpoint>")
    sc.hadoopConfiguration.set("fs.cos.service.access.key", "<your-cos-access-key>")
    sc.hadoopConfiguration.set("fs.cos.service.secret.key", "<your-cos-secret-key>")
    ```

### Usage (IBM)

```scala
spark.range(5).write.format("delta").save("cos://<your-cos-bucket>.service/<path-to-delta-table>")
spark.read.format("delta").load("cos://<your-cos-bucket>.service/<path-to-delta-table>").show()
```

.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark