---
description: Learn about type widening in Delta.
---

# Delta type widening

.. note:: This feature is available in preview in <Delta> 3.2 and above.

The type widening feature allows changing the type of columns in a Delta table to a wider type. This enables manual type changes using the `ALTER TABLE ALTER COLUMN` command and automatic type migration with schema evolution in `INSERT` and `MERGE INTO` commands.

## Supported type changes

The feature introduces a limited set of supported type changes in <Delta> 3.2 and expands it in <Delta> 4.0 and above.

.. csv-table::
  :header: "Source type", "Supported wider types - Delta 3.2", "Supported wider types - Delta 4.0"

  "`byte`","`short`, `int`","`short`,`int`,`long`"
  "`short`","`int`", "`int`,`long`"
  "`int`"," ","`long`"
  "`float`", ,"`double`"
  "`decimal`", ,"`decimal` with greater precision and same scale"
  "`date`", ,"`timestampNTZ`"

Type changes are supported for top-level columns as well as fields nested inside structs, maps and arrays.

## How to enable <Delta> type widening

.. important::

  Enabling type widening sets the Delta table feature `typeWidening-preview`, a reader/writer protocol feature. Only clients that support this table feature can read and write to the table once the table feature is set. You must use <Delta> 3.2 or above to read and write to such Delta tables.

You can enable type widening on an existing table by setting the `delta.enableTypeWidening` table property to `true`:

  ```sql
  ALTER TABLE <table_name> SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
  ```

Alternatively, you can enable type widening during table creation:

  ```sql
  CREATE TABLE <table_name> USING DELTA TBLPROPERTIES('delta.enableTypeWidening' = 'true')
  ```

To disable type widening:

  ```sql
  ALTER TABLE <table_name> SET TBLPROPERTIES ('delta.enableTypeWidening' = 'false')
  ```

Disabling type widening prevents future type changes from being applied to the table. It doesn't affect type changes previously applied and in particular, it doesn't remove the type widening table feature and doesn't allow clients that don't support the type widening table feature to read and write to the table.

To remove the type widening table feature from the table and allow other clients that don't support this feature to read and write to the table, see [_](#removing-the-type-widening-table-feature).

## Manually applying a type change

When type widening is enabled on a Delta table, you can change the type of a column using the `ALTER COLUMN` command:

```sql
ALTER TABLE <table_name> ALTER COLUMN <col_name> TYPE <new_type>
```

The table schema is updated without rewriting the underlying Parquet files.

## Type changes with automatic schema evolution
Type changes are applied automatically during ingestion with `INSERT` and `MERGE INTO` commands when:
- Type widening is enabled on the target table.
- The command runs with [automatic schema evolution](delta-update.md#merge-schema-evolution) enabled.
- The source column type is wider than the target column type.
- Changing the target column type to the source type is a [supported type change](#supported-type-changes)
- The type change is not a promotion from integers to decimal/double: `byte`, `short`, `int`, or `long` to `decimal` or `double`.

When all conditions are satisfied, the target table schema is updated automatically to change the target column type to the source column type.

## Removing the type widening table feature

The type widening feature can be removed from a Delta table using the `DROP FEATURE` command:

```sql
 ALTER TABLE <table-name> DROP FEATURE 'typeWidening-preview' [TRUNCATE HISTORY]
```

See [_](delta-drop-feature.md) for more information on dropping Delta table features.

When dropping the type widening feature, the underlying Parquet files are rewritten when necessary to ensure that the column types in the files match the column types in the Delta table schema.
After the type widening feature is removed from the table, Delta clients that don't support the feature can read and write to the table.

.. include:: /shared/replacements.md
