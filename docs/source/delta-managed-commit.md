---
description: Learn about Delta Managed Commits.
---

# Delta Managed Commits

.. warning:: This feature is available in preview in <Delta> 4.0-preview. Since this feature is still in preview, it can undergo major changes. Furthermore, a table which has this preview feature enabled cannot be written to by future Delta releases until this feature is manually dropped from the table.

[Managed Commit](https://github.com/delta-io/delta/issues/2598) is a new commit protocol which makes the commit process more flexible and pluggable by giving control of the commit to an external commit owner. Each managed commit table has a designated "commit owner" and all the commits to the table must go via it.


# DynamoDB Commit Owner

<Delta> 4.0-preview also introduces a DynamoDB backed Commit Owner implementation.

## Quickstart Guide

### 1. Create the DynamoDB table
The DynamoDB Commit Owner needs a DynamoDB table at the backend to manage commits. You have the choice of creating the DynamoDB table yourself (recommended) or having it created for you automatically.

- Creating the DynamoDB table yourself

    This DynamoDB table will maintain commit metadata for multiple Delta tables, and it is important that it is configured with the [Read/Write Capacity Mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html) (for example, on-demand or provisioned) that is right for your use cases. As such, we strongly recommend that you create your DynamoDB table yourself. The following example uses the AWS CLI. To learn more, see the [create-table](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/create-table.html) command reference.

    ```bash
    aws dynamodb create-table \
    --region us-west-2 \
    --table-name dynamodb_delta_commit_owner \
    --attribute-definitions AttributeName=tableId,AttributeType=S
    --key-schema AttributeName=tableId,KeyType=HASH
    --billing-mode PAY_PER_REQUEST
    ```

- Automatic DynamoDB table creation

    When you create a DynamoDB Managed Table, the commit owner will try to create a backing DynamoDB table if it does not exist. This default table supports 5 strongly consistent reads and 5 writes per second. You may change these default values using the table-creation-only configurations keys `spark.databricks.delta.managedCommits.commitOwner.ddb.writeCapacityUnits` and `spark.databricks.delta.managedCommits.commitOwner.ddb.readCapacityUnits`.

### 2. Launch a spark shell with the right packages:

```bash
bin/spark-shell \
--packages io.delta:delta-spark_2.12:4.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.databricks.delta.managedCommits.commitOwner.ddb.awsCredentialsProviderName=<credentialsProviderName>
```

`<credentialsProviderName>` must be the fully qualified class name of the [AWS Credentials Provider](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html) (e.g. `com.amazonaws.auth.profile.ProfileCredentialsProvider`) which must be used for authenticating with the desired DynamoDB instance.

### 3. Create a table with DynamoDB as the commit owner by running the following command:

```sql
CREATE TABLE <table_name> (id string) USING DELTA
TBLPROPERTIES ('delta.managedCommits.commitOwner-preview' = 'dynamodb', 'delta.managedCommits.commitOwnerConf-preview' = '{\"managedCommitsTableName\"=\"<dynamodb_table_name>\",\"dynamoDBEndpoint\"=\"<dynamodb_region_endpoint>\"}');
```

Note that `managedCommits.commitOwnerConf-preview` is a serialized json with two top-level properties:
1. `managedCommitsTableName`: This is the name of the table which will be used by the commit owner client to store information about the table that it is managing. This is the same table that was created in step 1.
2. `dynamoDBEndpoint`: This must specify the fully-qualified url endpoint (e.g. `https://dynamodb.us-west-2.amazonaws.com`) of the DynamoDB instance. The full list of endpoints can be found [here](https://docs.aws.amazon.com/general/latest/gr/ddb.html).

Any future commit to this table will now by managed by the DynamoDB Commit Owner. You can also convert any existing non-managed commit table to managed commits by running:

```sql
ALTER TABLE <table_name>
SET TBLPROPERTIES ('delta.managedCommits.commitOwner-preview' = 'dynamodb', 'delta.managedCommits.commitOwnerConf-preview' = '{\"managedCommitsTableName\": \"<dynamodb_table_name>\",\"dynamoDBEndpoint\": \"<dynamodb_region_endpoint>\"}');
```

.. warning:: The commit that converts a table to a managed commit table goes through the configured `LogStore` directly. This means that the multi-cluster write restrictions imposed by the configured `LogStore` implementation still apply. To avoid corruption in filesystems where concurrent commits are not safe, no concurrent commits must be performed when the conversion to managed commits happens.

.. note:: Instead of specifying the table properties on every table creation, you can specify them as default table properties which will be used for every new table using spark configs. To do this, you can set the spark properties `spark.databricks.delta.properties.defaults.managedCommits.commitOwner-preview` and `spark.databricks.delta.properties.defaults.managedCommits.commitOwnerConf-preview`.


## Removing the Managed Commit Feature

The feature can be removed from a Delta table using the `DROP FEATURE` command:

```sql
ALTER TABLE <table-name> DROP FEATURE 'managed-commit-preview' [TRUNCATE HISTORY]
```

.. include:: /shared/replacements.md

## Compatibility

Managed Commit is a writer table feature, so only clients that understand the feature can write to these tables.
Older clients which do not understand this table feature can still read a managed commit table. However, the read may give stale results depending on table's [commit owner backfill policy](https://github.com/delta-io/delta/blob/branch-4.0-preview1/protocol_rfcs/managed-commits.md#commit-backfills). Note that the DynamoDB Commit Owner tries to backfill all commits immediately.


## Dependencies

The Managed Commit feature depends on two other features to work correctly --- [In Commit Timestamps](https://github.com/delta-io/delta/issues/2532) and [Vacuum Protocol Check](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#vacuum-protocol-check). Both these features will be enabled automatically (if not already enabled) when Managed Commit is enabled.