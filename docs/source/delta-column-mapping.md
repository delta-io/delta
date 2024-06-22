---
description: Learn about column mapping in Delta.
---

# Delta column mapping

.. note:: This feature is available in <Delta> 1.2.0 and above. This feature is currently experimental with [known limitations](#known-limitations).

[Column mapping feature](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping) allows Delta table columns and the underlying Parquet file columns to use different names. This enables Delta schema evolution operations such as `RENAME COLUMN` and `DROP COLUMNS` on a Delta table without the need to rewrite the underlying Parquet files. It also allows users to name Delta table columns by using [characters that are not allowed](#supported-characters-in-column-names) by Parquet, such as spaces, so that users can directly ingest CSV or JSON data into Delta without the need to rename columns due to previous character constraints.

## How to enable <Delta> column mapping

.. important:: Enabling column mapping for a table upgrades the Delta [table version](versioning.md). This protocol upgrade is irreversible. Tables with column mapping enabled can only be read in <Delta> 1.2 and above.

Column mapping requires the following Delta protocols:

- Reader version 2 or above.
- Writer version 5 or above.

For a Delta table with the required protocol versions, you can enable column mapping by setting `delta.columnMapping.mode` to `name`.

You can use the following command to upgrade the table version and enable column mapping:

  ```sql
  ALTER TABLE <table_name> SET TBLPROPERTIES (
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
  )
  ```

.. note:: You cannot turn off column mapping after you enable it. If you try to set `'delta.columnMapping.mode' = 'none'`, you'll get an error.


## Rename a column

When column mapping is enabled for a Delta table, you can rename a column:

```sql
ALTER TABLE <table_name> RENAME COLUMN old_col_name TO new_col_name
```

For more examples, see [_](/delta-batch.md#rename-columns).

## Drop columns

When column mapping is enabled for a Delta table, you can drop one or more columns:

```sql
ALTER TABLE table_name DROP COLUMN col_name
ALTER TABLE table_name DROP COLUMNS (col_name_1, col_name_2, ...)

```

For more details, see [_](/delta-batch.md#drop-columns).

## Supported characters in column names

When column mapping is enabled for a Delta table, you can include spaces as well as any of these characters in the table's column names: `,;{}()\n\t=`.

## Known limitations
- Enabling column mapping on tables might break downstream operations that rely on Delta change data feed. See [_](/delta-change-data-feed.md#column-mapping-limitations).
- In <Delta> 2.1 and below, [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) reads are explicitly blocked on a column mapping enabled table.
- In <Delta> 2.2 and above, [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) reads are explicitly blocked on a column mapping enabled table that underwent column renaming or column dropping.
- In <Delta> 3.0 and above, [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) reads require schema tracking to be enabled on a column mapping enabled table that underwent column renaming or column dropping. See [_](/delta-streaming.md#schema-tracking)
- The Delta table protocol specifies two modes of column mapping, by `name` and by `id`. <Delta> 2.1 and below do not support `id` mode.

.. include:: /shared/replacements.md
