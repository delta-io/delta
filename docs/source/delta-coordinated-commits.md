---
description: Learn about Delta Coordinated Commits.
---

# Delta Coordinated Commits

.. warning:: This feature is available in preview in <Delta> 4.0.0 Preview. Since this feature is still in preview, it can undergo major changes. Furthermore, a table with this preview feature enabled cannot be written to by future Delta releases until the feature is manually removed from the table.

[Coordinated Commits](https://github.com/delta-io/delta/issues/2598) is a new commit protocol which makes the commit process more flexible and pluggable by delegating control of the commit to an external commit coordinator. Each coordinated commits table has a designated "commit coordinator" and all the commits to the table must go via it.


# DynamoDB Commit Coordinator

<Delta> 4.0.0 Preview also introduces a DynamoDB backed Commit Coordinator implementation.

## Quickstart Guide

### 1. Create the DynamoDB table
The DynamoDB Commit Coordinator requires a backend DynamoDB table to coordinate commits. One DynamoDB table can be used to manage multiple Delta tables. You have the choice of creating the DynamoDB table yourself (recommended) or having it created for you automatically.

- Creating the DynamoDB table yourself

    This DynamoDB table will maintain commit metadata for multiple Delta tables, and it is important that it is configured with the [Read/Write Capacity Mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html) (for example, on-demand or provisioned) that is right for your use cases. As such, we strongly recommend that you create your DynamoDB table yourself. The following example uses the AWS CLI. To learn more, see the [create-table](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/create-table.html) command reference.

    ```bash
    aws dynamodb create-table \
    --region us-west-2 \
    --table-name dynamodb_delta_commit_coordinator \
    --attribute-definitions AttributeName=tableId,AttributeType=S
    --key-schema AttributeName=tableId,KeyType=HASH
    --billing-mode PAY_PER_REQUEST
    ```

- Automatic DynamoDB table creation

    When you create a DynamoDB Coordinated Table, the commit coordinator will try to create a backing DynamoDB table if it does not exist. This default table supports 5 strongly consistent reads and 5 writes per second. You may change these default values using the table-creation-only configurations keys `spark.databricks.delta.coordinatedCommits.commitCoordinator.dynamodb.writeCapacityUnits` and `spark.databricks.delta.coordinatedCommits.commitCoordinator.dynamodb.readCapacityUnits`.

### 2. Launch a spark shell with the right packages:

```bash
bin/spark-shell \
--packages io.delta:delta-spark_2.13:4.0.0rc1,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.databricks.delta.coordinatedCommits.commitCoordinator.ddb.awsCredentialsProviderName=<credentialsProviderName>
```

`<credentialsProviderName>` must be the fully qualified class name of the [AWS Credentials Provider](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html) (e.g. `com.amazonaws.auth.profile.ProfileCredentialsProvider`) which must be used for authenticating with the desired DynamoDB instance. The exact delta-spark package name and repository can vary depending on the release that you are trying to use.

### 3. Create a table with DynamoDB as the commit coordinator by running the following command:

```sql
CREATE TABLE <table_name> (id string) USING DELTA
TBLPROPERTIES ('delta.coordinatedCommits.commitCoordinator-preview' = 'dynamodb', 'delta.coordinatedCommits.commitCoordinatorConf-preview' = '{\"dynamoDBTableName\": \"<dynamodb_table_name>\",\"dynamoDBEndpoint\": \"<dynamodb_region_endpoint>\"}');
```

Note that `coordinatedCommits.commitCoordinatorConf-preview` is a serialized JSON with two top-level properties:
1. `dynamoDBTableName`: This is the name of the table which will be used by the commit coordinator client to store information about the table that it is managing. This is the same table that was created in step 1.
2. `dynamoDBEndpoint`: This must specify the fully-qualified url endpoint (e.g. `https://dynamodb.us-west-2.amazonaws.com`) of the DynamoDB instance. The full list of endpoints can be found [here](https://docs.aws.amazon.com/general/latest/gr/ddb.html).

Any future commit to this table will now by coordinated by the DynamoDB Commit Coordinator. You can also convert any existing non-coordinated commit table to coordinated commits by running:

```sql
ALTER TABLE <table_name>
SET TBLPROPERTIES ('delta.coordinatedCommits.commitCoordinator-preview' = 'dynamodb', 'delta.coordinatedCommits.commitCoordinatorConf-preview' = '{\"dynamoDBTableName\": \"<dynamodb_table_name>\",\"dynamoDBEndpoint\": \"<dynamodb_region_endpoint>\"}');
```

.. warning:: The commit that converts a table to a coordinated commit table goes through the configured `LogStore` directly. This means the multi-cluster write restrictions imposed by the configured LogStore implementation still apply. To avoid corruption in filesystems where concurrent commits are not safe, no concurrent commits must be performed when the conversion to coordinated commits happens.

.. note:: Instead of specifying the table properties for each table creation, you can set them as default table properties to be used for every new table via Spark configurations. To do this, you can set the spark properties `spark.databricks.delta.properties.defaults.coordinatedCommits.commitCoordinator-preview` and `spark.databricks.delta.properties.defaults.coordinatedCommits.commitCoordinatorConf-preview`.


## Removing the Coordinated Commits Feature

The feature can be removed from a Delta table by using the `DROP FEATURE` command:

```sql
ALTER TABLE <table-name> DROP FEATURE 'coordinatedCommits-preview' [TRUNCATE HISTORY]
```

.. include:: /shared/replacements.md

## Compatibility

Coordinated Commits is a writer table feature, so only clients that recognize the feature can write to these tables.
Older clients which do not understand this table feature can still read a coordinated commits table. However, the read may give stale results depending on table's [commit coordinator backfill policy](https://github.com/delta-io/delta/blob/branch-4.0-preview1/protocol_rfcs/coordinated-commits.md#commit-backfills). Note that the DynamoDB Commit Coordinator tries to backfill all commits immediately.


## Dependencies

The Coordinated Commits feature depends on two other features to function correctly --- [In Commit Timestamps](https://github.com/delta-io/delta/issues/2532) and [Vacuum Protocol Check](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#vacuum-protocol-check). These features will be enabled automatically (if not already enabled) when Coordinated Commits is activated.
