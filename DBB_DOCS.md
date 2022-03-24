# DO NOT MERGE - delta.io docs for review

- NOTE: Amazon S3 section is already in the delta.io website here: https://docs.delta.io/latest/delta-storage.html#amazon-s3
- NOTE: Please just review the section Amazon S3 (with multi-cluster writes)

## Amazon S3

<Delta> has built-in support for S3. <Delta> supports concurrent reads from multiple clusters, but concurrent writes to S3 must originate from a _single_ Spark driver in order for <Delta> to provide transactional guarantees. This is because S3 currently does provide mutual exclusion, that is, there is no way to ensure that only one writer is able to create a file.

.. warning:: Concurrent writes to the same Delta table from multiple Spark drivers can lead to data loss.

.. contents:: In this section:
:local:
:depth: 1

### Requirements

- S3 credentials: [IAM roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) (recommended) or access keys
- <AS> associated with the corresponding <Delta> version.
- Hadoop's [AWS connector (hadoop-aws)](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/) for the version of Hadoop that <AS> is compiled for.

### Quickstart

This section explains how to quickly start reading and writing Delta tables on S3. For a detailed explanation of the configuration, see [_](#configuration).

#. Use the following command to launch a Spark shell with <Delta> and S3 support (assuming you use Spark pre-built for Hadoop 3.2):

   ```bash
   bin/spark-shell \
    --packages io.delta:delta-core_2.12:$VERSION$,org.apache.hadoop:hadoop-aws:3.2.0 \
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

For other languages and more examples of Delta table operations, see [_](quick-start.md).

### Configuration

Here are the steps to configure <Delta> for S3.

#. Include `hadoop-aws` JAR in the classpath.

   <Delta> needs the `org.apache.hadoop.fs.s3a.S3AFileSystem` class from the `hadoop-aws` package, which implements Hadoop's `FileSystem` API for S3. Make sure the version of this package matches the Hadoop version with which Spark was built.

#. Set up S3 credentials.

We recommend using [IAM roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) for authentication and authorization. But if you want to use keys, here is one way is to set up the [Hadoop configurations](https://spark.apache.org/docs/latest/configuration.html#custom-hadoophive-configuration) (in Scala):

   ```scala
   sc.hadoopConfiguration.set("fs.s3a.access.key", "<your-s3-access-key>")
   sc.hadoopConfiguration.set("fs.s3a.secret.key", "<your-s3-secret-key>")
   ```

## Amazon S3 (with multi-cluster writes)

.. note::
This support is new and experimental.

As mentioned in [Amazon S3](#amazon-s3), out-of-the-box <Delta> only supports concurrent writes to S3 from a single Spark driver. However, multi-cluster write support to S3 can be obtained by configuring <Delta> to use the right `LogStore` implementation.

This implementation, with class name `DynamoDBLogStore`, uses [DynamoDB](https://aws.amazon.com/dynamodb/) to provide the mutual exclusion that S3 is inherently lacking.

.. warning::
This multi-cluster writing solution is only safe when all writers use this `LogStore` implementation. Concurrent writes to the same Delta table from multiple Spark drivers with some drivers using out-of-the-box <Delta> and others using this experimental `LogStore` can lead to data loss.

### Requirements
- All of the requirements listed in [Amazon S3](#amazon-s3)'s Requirements section
- JAR of the [delta-storage-dynamodb artifact](https://todo)
- JAR of the [AWS SDK for Java](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk)

### Configuration

.. warning::
Misconfiguring this `LogStore` implementation (e.g., specifying the wrong DynamoDB table) can lead to data loss.

#. First, follow the configuration steps listed in [Amazon S3](#amazon-s3)'s Configuration section

#. Configure LogStore implementation for the scheme `s3`.

   ```ini
   spark.delta.logstore.s3.impl=io.delta.storage.DynamoDBLogStore
   ```

#. Include the JARs for `delta-storage-dynamodb` and `aws-java-sdk` in the classpath.

#. Configure the `LogStore` implementation in your Spark Session.

Each Spark Session will need to be configured with information necessary to instantiate the DynamoDB client.

A list of per-session configurations and their defaults is given below:

   ```
   - spark.delta.DynamoDBLogStore.tableName - table name (defaults to 'delta_log')
   - spark.delta.DynamoDBLogStore.endpoint - endpoint (defaults to 'Amazon AWS')
   - spark.delta.DynamoDBLogStore.region - AWS region (defaults to 'us-east-1')
   - spark.delta.DynamoDBLogStore.credentials.provider - name of class implementing
     `com.amazonaws.auth.AWSCredentialsProvider` (defaults to 'DefaultAWSCredentialsProviderChain')
   ```

#. (Optional) Create the DynamoDB table yourself.

A single DynamoDB table will maintain commit metadata for multiple Delta tables, and it is important that your DynamoDB table is configured with sufficient [Read and Write Capacity Units](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html#HowItWorks.ProvisionedThroughput.Manual).

If the default DynamoDB table does not already exist then it will be created for you automatically. This default table supports 5 strongly consistent reads and 5 writes per second. You may change these default values using the configurations below:

   ```
   - spark.delta.DynamoDBLogStore.provisionedThroughput.rcu - Read Capacity Units (defaults to 5)
   - spark.delta.DynamoDBLogStore.provisionedThroughput.wcu - Write Capacity Units (defaults to 5)
   ```

Alternatively, you can create the DynamoDB table yourself. An example is provided below using the AWS CLI. Learn more [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/getting-started-step-1.html).

   ```bash
   aws \
     --region us-west-2 dynamodb create-table \
     --table-name delta_log \
     --attribute-definitions AttributeName=tablePath,AttributeType=S \
                             AttributeName=fileName,AttributeType=S \
     --key-schema AttributeName=tablePath,KeyType=HASH \
                   AttributeName=fileName,KeyType=RANGE \
     --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
   ```

#. Cleanup old DynamoDB entries using Time to Live (TTL).

Once a DynamoDB metadata entry is marked as complete, and after sufficient time such that we can now rely on S3 alone to prevent accidental overwrites on the same Delta file, it is safe to delete that entry from DynamoDB. The cheapest way to do this is using [DynamoDB's TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html) feature which is a free, automated means to delete items from your table.

Run the following command on your given DynamoDB table to enable TTL:

   ```bash
   aws dynamodb \
     --region us-west-2 update-time-to-live \
     --table-name delta_log \
     --time-to-live-specification "Enabled=true, AttributeName=commitTime"
   ```

#. Cleanup old AWS S3 temp files using S3 Lifecycle Expiration.

In this `LogStore` implementation, a temp file is created containing a copy of the metadata to be committed into the Delta log. Once that commit to the Delta log is complete, and after the corresponding DynamoDB entry has been removed, it is safe to delete this temp file.

We suggest deleting temp files using an [S3 Lifecycle Expiration rule](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html), with filter prefix pointing to the `/_delta_log/.tmp` folder located in your table path, and an expiration value of 3 days.

One way to do this is using S3's `put-bucket-lifecycle-configuration` command. Learn more [here](https://docs.aws.amazon.com/cli/latest/reference/s3api/put-bucket-lifecycle-configuration.html). An example rule and command invocation is given below:

   ```json
   // file://lifecycle.json
   {
     "Rules":[
       {
         "ID":"expire_tmp_files",
         "Filter":{
           "Prefix":"/path/to/table/_delta_log/.tmp"
         },
         "Status":"Enabled",
         "Expiration":{
           "Days":3
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
Currently AWS S3 has a hard limit of 1000 rules per bucket.