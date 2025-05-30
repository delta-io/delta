---
description: Learn how to set up an integration to enable you to read Delta tables from <Snowflake>.
---

# <Snowflake> connector

Visit the [Snowflake Delta Lake support](https://docs.snowflake.com/en/user-guide/tables-external-intro.html#delta-lake-support) documentation to use the connector.

.. important::

  ### What to do if Snowflake performance is slow when reading Delta tables?

  Some users in the community have reported that <Snowflake>, unlike [Trino](https://trino.io/docs/current/connector/delta-lake.html) or [Spark](delta-batch.md), is not using [Delta statistics](optimizations-oss.md#data-skipping) to do data skipping when reading Delta tables. Due to this bug, <Snowflake> may read a lot of unnecessary parquet files resulting in poor query performance and increased API call requests from cloud providers.

  If you are a <Snowflake> customer and have subscribed to their enterprise support, please [open a support case](https://community.snowflake.com/s/article/How-To-Submit-a-Support-Case-in-Snowflake-Lodge).

  As a workaround, you can enable [Delta UniForm](delta-uniform.md) to generate Iceberg metadata and read these tables as Iceberg tables from <Snowflake>.

## Integration using manifest files (obsolete)

A Delta table can be read by <Snowflake> using a _manifest file_, which is a text file containing the list of data files to read for querying a Delta table. This article describes how to set up a <Delta> to <Snowflake> integration using manifest files and query Delta tables.

### Set up a <Delta> to <Snowflake> integration and query Delta tables

You set up a <Delta> to <Snowflake> integration using the following steps.

#### Step 1: Generate manifests of a Delta table using <AS>

Run the `generate` operation on a Delta table at location `<path-to-delta-table>`:

.. code-language-tabs::

  ```sql
  GENERATE symlink_format_manifest FOR TABLE delta.`<path-to-delta-table>`
  ```

  ```scala
  val deltaTable = DeltaTable.forPath(<path-to-delta-table>)
  deltaTable.generate("symlink_format_manifest")
  ```

  ```java
  DeltaTable deltaTable = DeltaTable.forPath(<path-to-delta-table>);
  deltaTable.generate("symlink_format_manifest");
  ```

  ```python
  deltaTable = DeltaTable.forPath(<path-to-delta-table>)
  deltaTable.generate("symlink_format_manifest")
  ```

See [Generate a manifest file](delta-utility.md#delta-generate) for details.


The `generate` operation generates manifest files at `<path-to-delta-table>/_symlink_format_manifest/`. In other words, the files in this directory contain the names of the data files (that is, Parquet files) that should be read for reading a snapshot of the Delta table.

.. note::
  We recommend that you define the Delta table in a location that <Snowflake> can read directly.

#### Step 2: Configure <Snowflake> to read the generated manifests

Run the following commands in your <Snowflake> environment.

##### Define an external table on the manifest files

To define an external table in <Snowflake>, you must first [define a external stage](https://docs.snowflake.net/manuals/user-guide/data-load-s3-create-stage.html) `my_staged_table` that points to the Delta table. In <Snowflake>, run the following.

```sql
create or replace stage my_staged_table url='<path-to-delta-table>'
```

Replace `<path-to-delta-table>` with the full path to the Delta table. Using this stage, you can [define a table](https://docs.snowflake.net/manuals/sql-reference/sql/create-external-table.html) `delta_manifest_table` that reads the file names specified in the manifest files as follows:

```sql
CREATE OR REPLACE EXTERNAL TABLE delta_manifest_table(
    filename VARCHAR AS split_part(VALUE:c1, '/', -1)
  )
  WITH LOCATION = @my_staged_table/_symlink_format_manifest/
  FILE_FORMAT = (TYPE = CSV)
  PATTERN = '.*[/]manifest'
  AUTO_REFRESH = true;
```

.. note:: In this query:

  - The location is the manifest subdirectory.
  - The `filename` column contains the name of the files (not the full path) defined in the manifest.

##### Define an external table on Parquet files

You can define a table `my_parquet_data_table` that reads all the Parquet files in the Delta table.

```sql
CREATE OR REPLACE EXTERNAL TABLE my_parquet_data_table(
    id INT AS (VALUE:id::INT),
    part INT AS (VALUE:part::INT),
    ...
    parquet_filename VARCHAR AS split_part(metadata$filename, '/', -1)
  )
  WITH LOCATION = @my_staged_table/
  FILE_FORMAT = (TYPE = PARQUET)
  PATTERN = '.*[/]part-[^/]*[.]parquet'
  AUTO_REFRESH = true;
```

.. note:: In this query:

  - The location is the Delta table path.
  - The `parquet_filename` column contains the name of the file that contains each row of the table.

If your Delta table is partitioned, then you will have to explicitly extract the partition values in the table definition. For example, if the table was partitioned by a single integer column named `part`, you can extract the values as follows:

```sql
CREATE OR REPLACE EXTERNAL TABLE my_parquet_data_partitioned_table(
    id INT AS (VALUE:id::INT),
    part INT AS (
        nullif(
            regexp_replace (metadata$filename, '.*part\\=(.*)\\/.*', '\\1'),
            '__HIVE_DEFAULT_PARTITION__'
        )::INT
    ),
    ...
    parquet_filename VARCHAR AS split_part(metadata$filename, '/', -1),
  )
  WITH LOCATION = @my_staged_partitioned_table/
  FILE_FORMAT = (TYPE = PARQUET)
  PATTERN = '.*[/]part-[^/]*[.]parquet'
  AUTO_REFRESH = true;
```

The regular expression is used to extract the partition value for the column `part`.

Querying the Delta table as this Parquet table will produce incorrect results because this query will read all the Parquet files in this table rather than only those that define a consistent snapshot of the table. You can use the manifest table to get a consistent snapshot data.

##### Define view to get correct contents of the Delta table using the manifest table

To read only the rows belonging to the consistent snapshot defined in the generated manifests, you can apply a filter to keep only the rows in the Parquet table that came from the files defined in the manifest table.

```sql
CREATE OR REPLACE VIEW my_delta_table AS
    SELECT id, part, ...
    FROM my_parquet_data_table
    WHERE parquet_filename IN (SELECT filename FROM delta-manifest-table);
```

Querying this view will provide you with a [consistent](#data-consistency) view of the Delta table.

#### Step 3: Update manifests

When data in a Delta table is updated, you must regenerate the manifests using either of the following approaches:

- **Update explicitly**: After all the data updates, you can run the `generate` operation to update the manifests.
- **Update automatically**: You can configure a Delta table so that all write operations on the table automatically update the manifests. To enable this automatic mode, set the corresponding table property using the following SQL command.

  ```sql
  ALTER TABLE delta.`<path-to-delta-table>` SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)
  ```

### Limitations

The <Snowflake> integration has known limitations in its behavior.

#### Data consistency

Whenever <Delta> generates updated manifests, it atomically overwrites existing manifest files. Therefore, <Snowflake> will always see a consistent view of the data files; it will see all of the old version files or all of the new version files. However, the granularity of the consistency guarantees depends on whether the table is partitioned or not.

- **Unpartitioned tables**: All the files names are written in one manifest file which is updated atomically. In this case <Snowflake> will see full table snapshot consistency.

- **Partitioned tables**: A manifest file is partitioned in the same Hive-partitioning-style directory structure as the original Delta table. This means that each partition is updated atomically, and <Snowflake> will see a consistent view of each partition but not a consistent view across partitions. Furthermore, since all manifests of all partitions cannot be updated together, concurrent attempts to generate manifests can lead to different partitions having manifests of different versions.

Depending on what storage system you are using for Delta tables, it is possible to get incorrect results when <Snowflake> concurrently queries the manifest while the manifest files are being rewritten. In file system implementations that lack atomic file overwrites, a manifest file may be momentarily unavailable. Hence, use manifests with caution if their updates are likely to coincide with queries from <Snowflake>.

#### Performance

This is an experimental integration and its performance and scalability characteristics have not yet been tested.

#### Schema evolution

<Delta> supports schema evolution and queries on a Delta table automatically use the latest schema regardless of the schema defined in the table in the Hive metastore. However, <Snowflake> uses the schema defined in its table definition, and will not query with the updated schema until the table definition is updated to the new schema.

.. <Snowflake> replace:: Snowflake
.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark