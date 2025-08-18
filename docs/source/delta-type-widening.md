---
description: Learn about type widening in Delta.
---

# Delta type widening

.. note:: This feature is available in preview in <Delta> 3.2 and above, and fully supported in <Delta> 4.0 and above.

The type widening feature allows changing the type of columns in a Delta table to a wider type. This enables manual type changes using the `ALTER TABLE ALTER COLUMN` command and automatic type migration with schema evolution during write operations.

## Supported type changes

The feature introduces a limited set of supported type changes in <Delta> 3.2 and expands it in <Delta> 4.0 and above.

.. csv-table::
  :header: "Source type", "Supported wider types - Delta 3.2", "Supported wider types - Delta 4.0"

  "`byte`","`short`, `int`","`short`,`int`,`long`, `decimal`, `double`"
  "`short`","`int`", "`int`,`long`, `decimal`, `double`"
  "`int`"," ","`long`, `decimal`, `double`"
  "`long`", ,"`decimal`"
  "`float`", ,"`double`"
  "`decimal`", ,"`decimal` with greater precision and scale"
  "`date`", ,"`timestampNTZ`"

To avoid accidentally promoting integer values to decimals, you must **manually commit** type changes from `byte`, `short`, `int`, or `long` to `decimal` or `double`. When promoting an integer type to `decimal` or `double`, if any downstream ingestion writes this value back to an integer column, Spark will truncate the fractional part of the values by default.

.. note::

  When changing an integer or decimal type to decimal, the total precision must be equal to or greater than the starting precision. If you also increase the scale, the total precision must increase by a corresponding amount.
  That is, `decimal(p, s)` can be changed to `decimal(p + k1, s + k2)` iff `k1 >= k2 >= 0`.

  For example, if you want to add two decimal places to a field with `decimal(10,1)`, the minimum target is `decimal(12,3)`.

  The minimum target for `byte`, `short`, and `int` types is `decimal(10,0)`. The minimum target for `long` is `decimal(20,0)`.

Type changes are supported for top-level columns as well as fields nested inside structs, maps and arrays.

## How to enable <Delta> type widening

.. important::

  Enabling type widening using <Delta> 3.3 and above sets the Delta table feature `typeWidening`, a reader/writer protocol feature. Only clients that support this table feature can read and write to the table once the table feature is set. You must use <Delta> 3.3 or above to read and write to such Delta tables.
  
  Enabling type widening using <Delta> 3.2 sets the Delta table feature `typeWidening-preview` on the table instead. You must use <Delta> 3.2 or above to read and write to such Delta tables.

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

Schema evolution works with type widening to update data types in target tables to match the type of incoming data.

.. note::

  Without type widening enabled, schema evolution always attempts to downcast data to match column types in the target table. If you don't want to automatically widen data types in your target tables, disable type widening before you run workloads with schema evolution enabled.

To use schema evolution to widen the data type of a column during ingestion, you must meet the following conditions:

- The write command runs with automatic schema evolution enabled.
- The target table has type widening enabled.
- The source column type is wider than the target column type.
- Type widening supports the type change.
- The type change is not one of `byte`, `short`, `int`, or `long` to `decimal` or `double`. These type changes can only be applied manually using ALTER TABLE to avoid accidental promotion of integers to decimals.

Type mismatches that don't meet all of these conditions follow normal schema enforcement rules.

## <a id="drop"></a> Removing the type widening table feature

The type widening feature can be removed from a Delta table using the `DROP FEATURE` command:

```sql
 ALTER TABLE <table_name> DROP FEATURE 'typeWidening' [TRUNCATE HISTORY]
```

.. note::

  Tables that enabled type widening using <Delta> 3.2 require dropping feature `typeWidening-preview` instead.

See [_](delta-drop-feature.md) for more information on dropping Delta table features.

When dropping the type widening feature, the underlying Parquet files are rewritten when necessary to ensure that the column types in the files match the column types in the Delta table schema.
After the type widening feature is removed from the table, Delta clients that don't support the feature can read and write to the table.

## Limitations

### Iceberg Compatibility

Iceberg doesn't support all type changes covered by type widening, see [Iceberg Schema Evolution](https://iceberg.apache.org/spec/#schema-evolution). In particular, Iceberg V2 does not support the following type changes:

- `byte`, `short`, `int`, `long` to `decimal` or `double`
- decimal scale increase
- `date` to `timestampNTZ`

When [UniForm with Iceberg compatibility](/delta-uniform.md) is enabled on a Delta table, applying one of these type changes results in an error.

If you apply one of these unsupported type changes to a Delta table, enabling [Uniform with Iceberg compatibility](/delta-uniform.md) on the table results in an error.
To resolve the error, you must [drop the type widening table feature](#drop).

.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark
