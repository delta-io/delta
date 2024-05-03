---
description: Learn how <Delta> row tracking allows tracking how rows change across table versions.
orphan: 1
---

# Use row tracking for Delta tables

Row tracking allows <Delta> to track row-level lineage in a <Delta> table. When enabled on a <Delta> table, row tracking adds two new metadata fields to the table:

- **Row IDs** provide rows with an identifier that is unique within the table. A row keeps the same ID whenever it is modified using a `MERGE` or `UPDATE` statement.

- **Row commit versions** record the last version of the table in which the row was modified. A row is assigned a new version whenever it is modified using a `MERGE` or `UPDATE` statement.

.. note:: This feature is available in <Delta> 3.2.0 and above. This feature is in experimental support mode with [_](#limitations).

## Enable row tracking

.. warning:: Tables created with row tracking enabled have the row tracking <Delta> table feature enabled at creation and use <Delta> writer version 7. Table protocol versions cannot be downgraded, and tables with row tracking enabled are not writeable by <Delta> clients that do not support all enabled <Delta> writer protocol table features. See [_](/versioning.md).

You must explicitly enable row tracking using one of the following methods:

- **New table**: Set the table property `delta.enableRowTracking = true` in the `CREATE TABLE` command.

  ```sql
  -- Create an empty table
  CREATE TABLE student (id INT, name STRING, age INT) TBLPROPERTIES ('delta.enableRowTracking' = 'true');

  -- Using a CTAS statement
  CREATE TABLE course_new
  TBLPROPERTIES ('delta.enableRowTracking' = 'true')
  AS SELECT * FROM course_old;

  -- Using a LIKE statement to copy configuration
  CREATE TABLE graduate LIKE student;

  -- Using a CLONE statement to copy configuration
  CREATE TABLE graduate CLONE student;
  ```

- **Existing table**: Set the table property `'delta.enableRowTracking' = 'true'` in the `ALTER TABLE` command.

  ```sql
  ALTER TABLE grade SET TBLPROPERTIES ('delta.enableRowTracking' = 'true');
  ```

- **All new tables**: Set the configuration `spark.databricks.delta.properties.defaults.enableRowTracking = true` for the current session in the `SET` command.

  .. code-language-tabs::
    ```sql
    SET spark.databricks.delta.properties.defaults.enableRowTracking = true;
    ```

    ```python
    spark.conf.set("spark.databricks.delta.properties.defaults.enableRowTracking", True)
    ```

    ```scala
    spark.conf.set("spark.databricks.delta.properties.defaults.enableRowTracking", true)
    ```

.. important:: Because cloning a <Delta> table creates a separate history, the row ids and row commit versions on cloned tables do not match that of the original table.

.. important:: Enabling row tracking on existing table will automatically assign row ids and row commit versions to all existing rows in the table. This process may cause multiple new versions of the table to be created and may take a long time.

### Row tracking storage

Enabling row tracking may increase the size of the table. <Delta> stores row tracking metadata fields in hidden metadata columns in the data files. Some operations, such as insert-only operations do not use these hidden columns and instead track the row ids and row commit versions using metadata in the <Delta> log. Data reorganization operations such as `OPTIMIZE` and `REORG` cause the row ids and row commit versions to be tracked using the hidden metadata column, even when they were stored using metadata.

## Read row tracking metadata fields

Row tracking adds the following metadata fields that can be accessed when reading a table:

| Column name                    | Type | Values                                                           |
|--------------------------------|------|------------------------------------------------------------------|
| `_metadata.row_id`             | Long | The unique identifier of the row.                                |
| `_metadata.row_commit_version` | Long | The table version at which the row was last inserted or updated. |

The row ids and row commit versions metadata fields are not automatically included when reading the table.
Instead, these metadata fields must be manually selected from the hidden `_metadata` column which is available for all tables in <AS>.

.. code-language-tabs::
  ```sql
  SELECT _metadata.row_id, _metadata.row_commit_version, * FROM table_name;
  ```

  ```python
  spark.read.table("table_name") \
    .select("_metadata.row_id", "_metadata.row_commit_version", "*")
  ```

  ```scala
  spark.read.table("table_name")
    .select("_metadata.row_id", "_metadata.row_commit_version", "*")
  ```

## Disable row tracking

Row tracking can be disabled to reduce the storage overhead of the metadata fields. After disabling row tracking the metadata fields remain available, but all rows always get assigned a new id and commit version whenever they are touched by an operation.

```sql
ALTER TABLE table_name SET TBLPROPERTIES (delta.enableRowTracking = false);
```

.. important:: Disabling row tracking does not remove the corresponding table feature and does not downgrade the table protocol version.

## Limitations

The following limitations exist:

- The row ids and row commit versions metadata fields cannot be accessed while reading the [Change data feed](/delta/delta-change-data-feed.md).
- Row Tracking can currently only be enabled when creating the table or when the table is empty. Enabling row tracking on a non-empty table is currently not supported.
- Once the Row Tracking feature is added to the table it cannot be removed without recreating the table.

.. include:: /shared/replacements.md
