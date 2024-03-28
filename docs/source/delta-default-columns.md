---
description: Learn about default column values in Delta.
---

# Delta default column values

.. note:: This feature is available in <Delta> 3.1.0 and above and is enabled using the `allowColumnDefaults` writer [table feature](#versioning).

Delta enables the specification of [default expressions](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#default-columns) for columns in Delta tables. When users write to these tables without explicitly providing values for certain columns, or when they explicitly use the DEFAULT SQL keyword for a column, Delta automatically generates default values for those columns.

This information is stored in the [StructField](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#struct-field) corresponding to the column of interest.

## How to enable <Delta> default column values

.. important:: Enabling default column values for a table upgrades the Delta [table version](versioning.md) as a byproduct of enabling [table features](#versioning). This protocol upgrade is irreversible. Tables with default column values enabled can only be written to in <Delta> 3.1 and above.

You can enable default column values for a table by setting `delta.feature.allowColumnDefaults` to `enabled`:

  ```sql
  ALTER TABLE <table_name> SET TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'enabled'
  )
  ```

## How to use default columns in SQL commands

- For SQL commands that perform table writes, such as `INSERT`, `UPDATE`, and `MERGE` commands, the `DEFAULT` keyword resolves to the most recently assigne default value for the corresponding column (or NULL if no default value exists). For instance, the following SQL command will use the default value for the second column in the table: `INSERT INTO t VALUES (16, DEFAULT);`

- It is also possible for INSERT commands to specify lists of fewer column than the target table, in which case the engine will assign default values for the remaining columns (or NULL for any columns where no defaults yet exist).

.. important:: The metadata discussed here apply solely to write operations, not read operations.

-  The `ALTER TABLE ... ADD COLUMN` command that introduces a new column to an existing table may not to specify a default value for the new column. For instance, the following SQL command is not supported in Delta Lake: `ALTER TABLE t ADD COLUMN c INT DEFAULT 16;`

- It is permissible, however, to assign or update default values for columns that were created in previous commands. For example, the following SQL command is valid: `ALTER TABLE t ALTER COLUMN c SET DEFAULT 16;`

 .. include:: /shared/replacements.md
