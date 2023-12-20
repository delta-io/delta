---
description: Learn how to delete data from and update data in Delta tables.
---

# Table deletes, updates, and merges
<Delta> supports several statements to facilitate deleting data from and updating data in Delta tables.

<a id="delta-delete"></a>

## Delete from a table

You can remove data that matches a predicate from a Delta table. For instance, in a table named `people10m` or a path at `/tmp/delta/people-10m`, to delete all rows corresponding to people with a value in the `birthDate` column from before `1955`, you can run the following:

.. code-language-tabs::

  .. lang:: sql

    ```sql
    DELETE FROM people10m WHERE birthDate < '1955-01-01'

    DELETE FROM delta.`/tmp/delta/people-10m` WHERE birthDate < '1955-01-01'
    ```


See [_](delta-batch.md#sql-support) for the steps to enable support for SQL commands.

.. code-language-tabs::

  .. lang:: python

    ```python
    from delta.tables import *
    from pyspark.sql.functions import *

    deltaTable = DeltaTable.forPath(spark, '/tmp/delta/people-10m')

    # Declare the predicate by using a SQL-formatted string.
    deltaTable.delete("birthDate < '1955-01-01'")

    # Declare the predicate by using Spark SQL functions.
    deltaTable.delete(col('birthDate') < '1960-01-01')
    ```

  .. lang:: scala

    ```scala
    import io.delta.tables._

    val deltaTable = DeltaTable.forPath(spark, "/tmp/delta/people-10m")

    // Declare the predicate by using a SQL-formatted string.
    deltaTable.delete("birthDate < '1955-01-01'")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // Declare the predicate by using Spark SQL functions and implicits.
    deltaTable.delete(col("birthDate") < "1955-01-01")
    ```

  .. lang:: java

    ```java
    import io.delta.tables.*;
    import org.apache.spark.sql.functions;

    DeltaTable deltaTable = DeltaTable.forPath(spark, "/tmp/delta/people-10m");

    // Declare the predicate by using a SQL-formatted string.
    deltaTable.delete("birthDate < '1955-01-01'");

    // Declare the predicate by using Spark SQL functions.
    deltaTable.delete(functions.col("birthDate").lt(functions.lit("1955-01-01")));
    ```

See the [_](delta-apidoc.md) for details.

.. important::
  `delete` removes the data from the latest version of the Delta table but does not remove it from the physical storage until the old versions are explicitly vacuumed. See [vacuum](delta-utility.md#delta-vacuum) for details.

.. tip::
  When possible, provide predicates on the partition columns for a partitioned Delta table as such predicates can significantly speed up the operation.

<a id="delta-update"></a>

## Update a table

You can update data that matches a predicate in a Delta table. For example, in a table named `people10m` or a path at `/tmp/delta/people-10m`, to change an abbreviation in the `gender` column from `M` or `F` to `Male` or `Female`, you can run the following:

.. code-language-tabs::

  .. lang:: sql

    ```sql
    UPDATE people10m SET gender = 'Female' WHERE gender = 'F';
    UPDATE people10m SET gender = 'Male' WHERE gender = 'M';

    UPDATE delta.`/tmp/delta/people-10m` SET gender = 'Female' WHERE gender = 'F';
    UPDATE delta.`/tmp/delta/people-10m` SET gender = 'Male' WHERE gender = 'M';
    ```


See [_](delta-batch.md#sql-support) for the steps to enable support for SQL commands.

.. code-language-tabs::

  .. lang:: python

    ```python
    from delta.tables import *
    from pyspark.sql.functions import *

    deltaTable = DeltaTable.forPath(spark, '/tmp/delta/people-10m')

    # Declare the predicate by using a SQL-formatted string.
    deltaTable.update(
      condition = "gender = 'F'",
      set = { "gender": "'Female'" }
    )

    # Declare the predicate by using Spark SQL functions.
    deltaTable.update(
      condition = col('gender') == 'M',
      set = { 'gender': lit('Male') }
    )
    ```

  .. lang:: scala

    ```scala
    import io.delta.tables._

    val deltaTable = DeltaTable.forPath(spark, "/tmp/delta/people-10m")

    // Declare the predicate by using a SQL-formatted string.
    deltaTable.updateExpr(
      "gender = 'F'",
      Map("gender" -> "'Female'")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // Declare the predicate by using Spark SQL functions and implicits.
    deltaTable.update(
      col("gender") === "M",
      Map("gender" -> lit("Male")));
    ```

  .. lang:: java

    ```java
    import io.delta.tables.*;
    import org.apache.spark.sql.functions;
    import java.util.HashMap;

    DeltaTable deltaTable = DeltaTable.forPath(spark, "/data/events/");

    // Declare the predicate by using a SQL-formatted string.
    deltaTable.updateExpr(
      "gender = 'F'",
      new HashMap<String, String>() {{
        put("gender", "'Female'");
      }}
    );

    // Declare the predicate by using Spark SQL functions.
    deltaTable.update(
      functions.col(gender).eq("M"),
      new HashMap<String, Column>() {{
        put("gender", functions.lit("Male"));
      }}
    );
    ```

See the [_](delta-apidoc.md) for details.

.. tip::
  Similar to delete, update operations can get a significant speedup with predicates on partitions.

<a id="delta-merge"></a>

## Upsert into a table using merge

You can upsert data from a source table, view, or DataFrame into a target Delta table by using the `MERGE` SQL operation. <Delta> supports inserts, updates and deletes in `MERGE`, and it supports extended syntax beyond the SQL standards to facilitate advanced use cases.

Suppose you have a source table named `people10mupdates` or a source path at `/tmp/delta/people-10m-updates` that contains new data for a target table named `people10m` or a target path at `/tmp/delta/people-10m`. Some of these new records may already be present in the target data. To merge the new data, you want to update rows where the person's `id` is already present and insert the new rows where no matching `id` is present. You can run the following:

.. code-language-tabs::

  .. lang:: sql

    ```sql
    MERGE INTO people10m
    USING people10mupdates
    ON people10m.id = people10mupdates.id
    WHEN MATCHED THEN
      UPDATE SET
        id = people10mupdates.id,
        firstName = people10mupdates.firstName,
        middleName = people10mupdates.middleName,
        lastName = people10mupdates.lastName,
        gender = people10mupdates.gender,
        birthDate = people10mupdates.birthDate,
        ssn = people10mupdates.ssn,
        salary = people10mupdates.salary
    WHEN NOT MATCHED
      THEN INSERT (
        id,
        firstName,
        middleName,
        lastName,
        gender,
        birthDate,
        ssn,
        salary
      )
      VALUES (
        people10mupdates.id,
        people10mupdates.firstName,
        people10mupdates.middleName,
        people10mupdates.lastName,
        people10mupdates.gender,
        people10mupdates.birthDate,
        people10mupdates.ssn,
        people10mupdates.salary
      )
    ```


See [_](delta-batch.md#sql-support) for the steps to enable support for SQL commands.

.. code-language-tabs::

  .. lang:: python

    ```python
    from delta.tables import *

    deltaTablePeople = DeltaTable.forPath(spark, '/tmp/delta/people-10m')
    deltaTablePeopleUpdates = DeltaTable.forPath(spark, '/tmp/delta/people-10m-updates')

    dfUpdates = deltaTablePeopleUpdates.toDF()

    deltaTablePeople.alias('people') \
      .merge(
        dfUpdates.alias('updates'),
        'people.id = updates.id'
      ) \
      .whenMatchedUpdate(set =
        {
          "id": "updates.id",
          "firstName": "updates.firstName",
          "middleName": "updates.middleName",
          "lastName": "updates.lastName",
          "gender": "updates.gender",
          "birthDate": "updates.birthDate",
          "ssn": "updates.ssn",
          "salary": "updates.salary"
        }
      ) \
      .whenNotMatchedInsert(values =
        {
          "id": "updates.id",
          "firstName": "updates.firstName",
          "middleName": "updates.middleName",
          "lastName": "updates.lastName",
          "gender": "updates.gender",
          "birthDate": "updates.birthDate",
          "ssn": "updates.ssn",
          "salary": "updates.salary"
        }
      ) \
      .execute()
    ```

  .. lang:: scala

    ```scala
    import io.delta.tables._
    import org.apache.spark.sql.functions._

    val deltaTablePeople = DeltaTable.forPath(spark, "/tmp/delta/people-10m")
    val deltaTablePeopleUpdates = DeltaTable.forPath(spark, "tmp/delta/people-10m-updates")
    val dfUpdates = deltaTablePeopleUpdates.toDF()

    deltaTablePeople
      .as("people")
      .merge(
        dfUpdates.as("updates"),
        "people.id = updates.id")
      .whenMatched
      .updateExpr(
        Map(
          "id" -> "updates.id",
          "firstName" -> "updates.firstName",
          "middleName" -> "updates.middleName",
          "lastName" -> "updates.lastName",
          "gender" -> "updates.gender",
          "birthDate" -> "updates.birthDate",
          "ssn" -> "updates.ssn",
          "salary" -> "updates.salary"
        ))
      .whenNotMatched
      .insertExpr(
        Map(
          "id" -> "updates.id",
          "firstName" -> "updates.firstName",
          "middleName" -> "updates.middleName",
          "lastName" -> "updates.lastName",
          "gender" -> "updates.gender",
          "birthDate" -> "updates.birthDate",
          "ssn" -> "updates.ssn",
          "salary" -> "updates.salary"
        ))
      .execute()
    ```

  .. lang:: java

    ```java
    import io.delta.tables.*;
    import org.apache.spark.sql.functions;
    import java.util.HashMap;

    DeltaTable deltaTable = DeltaTable.forPath(spark, "/tmp/delta/people-10m")
    Dataset<Row> dfUpdates = spark.read("delta").load("/tmp/delta/people-10m-updates")

    deltaTable
      .as("people")
      .merge(
        dfUpdates.as("updates"),
        "people.id = updates.id")
      .whenMatched()
      .updateExpr(
        new HashMap<String, String>() {{
          put("id", "updates.id");
          put("firstName", "updates.firstName");
          put("middleName", "updates.middleName");
          put("lastName", "updates.lastName");
          put("gender", "updates.gender");
          put("birthDate", "updates.birthDate");
          put("ssn", "updates.ssn");
          put("salary", "updates.salary");
        }})
      .whenNotMatched()
      .insertExpr(
        new HashMap<String, String>() {{
          put("id", "updates.id");
          put("firstName", "updates.firstName");
          put("middleName", "updates.middleName");
          put("lastName", "updates.lastName");
          put("gender", "updates.gender");
          put("birthDate", "updates.birthDate");
          put("ssn", "updates.ssn");
          put("salary", "updates.salary");
        }})
      .execute();
    ```

See the [_](delta-apidoc.md) for Scala, Java, and Python syntax details.

.. important::

  <Delta> merge operations typically require two passes over the source data. If your source data contains nondeterministic expressions, multiple passes on the source data can produce different rows causing incorrect results. Some common examples of nondeterministic expressions include the `current_date` and `current_timestamp` functions. In <Delta> 2.2 and above this issue is solved by automatically materializing the source data as part of the merge command, so that the source data is deterministic in multiple passes. In <Delta> 2.1 and below if you cannot avoid using non-deterministic functions, consider saving the source data to storage, for example as a temporary Delta table. Caching the source data may not address this issue, as cache invalidation can cause the source data to be recomputed partially or completely (for example when a cluster loses some of it executors when scaling down).

### Modify all unmatched rows using merge

.. note::
  `WHEN NOT MATCHED BY SOURCE` clauses are supported by the Scala, Python and Java [_](delta-apidoc.md) in Delta 2.3 and above. SQL is supported in Delta 2.4 and above.
  
You can use the `WHEN NOT MATCHED BY SOURCE` clause to `UPDATE` or `DELETE` records in the target table that do not have corresponding records in the source table. We recommend adding an optional conditional clause to avoid fully rewriting the target table.

The following code example shows the basic syntax of using this for deletes, overwriting the target table with the contents of the source table and deleting unmatched records in the target table.

.. code-language-tabs::

  ```python
  (targetDF
    .merge(sourceDF, "source.key = target.key")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
  )
  ```

  ```scala
  targetDF
    .merge(sourceDF, "source.key = target.key")
    .whenMatched()
    .updateAll()
    .whenNotMatched()
    .insertAll()
    .whenNotMatchedBySource()
    .delete()
    .execute()
  ```

  ```sql
  MERGE INTO target
  USING source
  ON source.key = target.key
  WHEN MATCHED
    UPDATE SET *
  WHEN NOT MATCHED
    INSERT *
  WHEN NOT MATCHED BY SOURCE
    DELETE
  ```

The following example adds conditions to the `WHEN NOT MATCHED BY SOURCE` clause and specifies values to update in unmatched target rows.

.. code-language-tabs::

  ```python
  (targetDF
    .merge(sourceDF, "source.key = target.key")
    .whenMatchedUpdate(
      set = {"target.lastSeen": "source.timestamp"}
    )
    .whenNotMatchedInsert(
      values = {
        "target.key": "source.key",
        "target.lastSeen": "source.timestamp",
        "target.status": "'active'"
      }
    )
    .whenNotMatchedBySourceUpdate(
      condition="target.lastSeen >= (current_date() - INTERVAL '5' DAY)",
      set = {"target.status": "'inactive'"}
    )
    .execute()
  )
  ```

  ```scala
  targetDF
    .merge(sourceDF, "source.key = target.key")
    .whenMatched()
    .updateExpr(Map("target.lastSeen" -> "source.timestamp"))
    .whenNotMatched()
    .insertExpr(Map(
      "target.key" -> "source.key",
      "target.lastSeen" -> "source.timestamp",
      "target.status" -> "'active'",
      )
    )
    .whenNotMatchedBySource("target.lastSeen >= (current_date() - INTERVAL '5' DAY)")
    .updateExpr(Map("target.status" -> "'inactive'"))
    .execute()
  ```

  ```sql
  MERGE INTO target
  USING source
  ON source.key = target.key
  WHEN MATCHED THEN
    UPDATE SET target.lastSeen = source.timestamp
  WHEN NOT MATCHED THEN
    INSERT (key, lastSeen, status) VALUES (source.key,  source.timestamp, 'active')
  WHEN NOT MATCHED BY SOURCE AND target.lastSeen >= (current_date() - INTERVAL '5' DAY) THEN
    UPDATE SET target.status = 'inactive'
  ```


### Operation semantics

Here is a detailed description of the `merge` programmatic operation.

- There can be any number of `whenMatched` and `whenNotMatched` clauses.

- `whenMatched` clauses are executed when a source row matches a target table row based on the match condition. These clauses have the following semantics.

  - `whenMatched` clauses can have at most one `update` and one `delete` action. The `update` action in `merge` only updates the specified columns (similar to the `update` [operation](#delta-update)) of the matched target row. The `delete` action deletes the matched row.

  - Each `whenMatched` clause can have an optional condition. If this clause condition exists, the `update` or `delete` action is executed for any matching source-target row pair only when the clause condition is true.

  - If there are multiple `whenMatched` clauses, then they are evaluated in the order they are specified. All `whenMatched` clauses, except the last one, must have conditions.

  - If none of the `whenMatched` conditions evaluate to true for a source and target row pair that matches the merge condition, then the target row is left unchanged.

  - To update all the columns of the target Delta table with the corresponding columns of the source dataset, use `whenMatched(...).updateAll()`. This is equivalent to:

    ```scala
    whenMatched(...).updateExpr(Map("col1" -> "source.col1", "col2" -> "source.col2", ...))
    ```

    for all the columns of the target Delta table. Therefore, this action assumes that the source table has the same columns as those in the target table, otherwise the query throws an analysis error.

    .. note::
      This behavior changes when automatic schema migration is enabled. See [Automatic schema evolution](#merge-schema-evolution) for details.

- `whenNotMatched` clauses are executed when a source row does not match any target row based on the match condition. These clauses have the following semantics.

  - `whenNotMatched` clauses can have only the `insert` action. The new row is generated based on the specified column and corresponding expressions. You do not need to specify all the columns in the target table. For unspecified target columns, `NULL` is inserted.

  - Each `whenNotMatched` clause can have an optional condition. If the clause condition is present, a source row is inserted only if that condition is true for that row. Otherwise, the source column is ignored.

  - If there are multiple `whenNotMatched` clauses, then they are evaluated in the order they are specified. All `whenNotMatched` clauses, except the last one, must have conditions.

  - To insert all the columns of the target Delta table with the corresponding columns of the source dataset, use `whenNotMatched(...).insertAll()`. This is equivalent to:

    ```scala
    whenNotMatched(...).insertExpr(Map("col1" -> "source.col1", "col2" -> "source.col2", ...))
    ```

    for all the columns of the target Delta table. Therefore, this action assumes that the source table has the same columns as those in the target table, otherwise the query throws an analysis error.

    .. note::
      This behavior changes when automatic schema migration is enabled. See [Automatic schema evolution](#merge-schema-evolution) for details.

- `whenNotMatchedBySource` clauses are executed when a target row does not match any source row based on the merge condition. These clauses have the following semantics.

  - `whenNotMatchedBySource` clauses can specify `delete` and `update` actions.

  - Each `whenNotMatchedBySource` clause can have an optional condition. If the clause condition is present, a target row is modified only if that condition is true for that row. Otherwise, the target row is left unchanged.

  - If there are multiple `whenNotMatchedBySource` clauses, then they are evaluated in the order they are specified. All `whenNotMatchedBySource` clauses, except the last one, must have conditions.

  - By definition, `whenNotMatchedBySource` clauses do not have a source row to pull column values from, and so source columns can't be referenced. For each column to be modified, you can either specify a literal or perform an action on the target column, such as `SET target.deleted_count = target.deleted_count + 1`.

.. important::

  - A `merge` operation can fail if multiple rows of the source dataset match and the merge attempts to update the same rows of the target Delta table. According to the SQL semantics of merge, such an update operation is ambiguous as it is unclear which source row should be used to update the matched target row. You can preprocess the source table to eliminate the possibility of multiple matches. See the [change data capture example](#write-change-data-into-a-Delta-table)---it shows how to preprocess the change dataset (that is, the source dataset) to retain only the latest change for each key before applying that change into the target Delta table.

  - You can apply a SQL `MERGE` operation on a SQL VIEW only if the view has been defined as `CREATE VIEW viewName AS SELECT * FROM deltaTable`.

### Schema validation

`merge` automatically validates that the schema of the data generated by insert and update expressions are compatible with the schema of the table. It uses the following rules to determine whether the `merge` operation is compatible:

- For `update` and `insert` actions, the specified target columns must exist in the target Delta table.

- For `updateAll` and `insertAll` actions, the source dataset must have all the columns of the target Delta table. The source dataset can have extra columns and they are ignored.


If you do not want the extra columns to be ignored and instead want to update the target table schema to include new columns, see [_](#merge-schema-evolution).

- For all actions, if the data type generated by the expressions producing the target columns are different from the corresponding columns in the target Delta table, `merge` tries to cast them to the types in the table.

<a id="merge-schema-evolution"></a>

### Automatic schema evolution

Schema evolution allows users to resolve schema mismatches between the target and source table in merge. It handles the following two cases:

#. A column in the source table is not present in the target table. The new column is added to the target schema, and its values are inserted or updated using the source values.
#. A column in the target table is not present in the source table. The target schema is left unchanged; the values in the additional target column are either left unchanged (for `UPDATE`) or set to `NULL` (for `INSERT`).

.. important:: To use schema evolution, you must set the Spark session configuration`spark.databricks.delta.schema.autoMerge.enabled` to `true` before you run the `merge` command.

  .. note::
    In Delta 2.3 and above, columns present in the source table can be specified by name in insert or update actions. In Delta 2.2 and below, only `INSERT *` or `UPDATE SET *` actions can be used for schema evolution with merge.

Here are a few examples of the effects of `merge` operation with and without schema evolution.

.. list-table::
    :header-rows: 1

    * - Columns
      - Query (in SQL)
      - Behavior without schema evolution (default)
      - Behavior with schema evolution
    * - Target columns: `key, value`

        Source columns: `key, value, new_value`
      - ```sql
        MERGE INTO target_table t
        USING source_table s
        ON t.key = s.key
        WHEN MATCHED
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *
        ```
      - The table schema remains unchanged; only columns `key`, `value` are updated/inserted.
      - The table schema is changed to `(key, value, new_value)`. Existing records with matches are updated with the `value` and `new_value` in the source. New rows are inserted with the schema `(key, value, new_value)`.

    * - Target columns: `key, old_value`

        Source columns: `key, new_value`
      - ```sql
        MERGE INTO target_table t
        USING source_table s
        ON t.key = s.key
        WHEN MATCHED
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *
        ```
      - `UPDATE` and `INSERT` actions throw an error because the target column `old_value` is not in the source.
      - The table schema is changed to `(key, old_value, new_value)`. Existing records with matches are updated with the `new_value` in the source leaving `old_value` unchanged. New records are inserted with the specified `key`, `new_value`, and `NULL` for the `old_value`.

    * - Target columns: `key, old_value`

        Source columns: `key, new_value`
      - ```sql
        MERGE INTO target_table t
        USING source_table s
        ON t.key = s.key
        WHEN MATCHED
          THEN UPDATE SET new_value = s.new_value
        ```
      - `UPDATE` throws an error because column `new_value` does not exist in the target table.
      - The table schema is changed to `(key, old_value, new_value)`. Existing records with matches are updated with the `new_value` in the source leaving `old_value` unchanged, and unmatched records have `NULL` entered for `new_value`.

    * - Target columns: `key, old_value`

        Source columns: `key, new_value`
      - ```sql
        MERGE INTO target_table t
        USING source_table s
        ON t.key = s.key
        WHEN NOT MATCHED
          THEN INSERT (key, new_value) VALUES (s.key, s.new_value)
        ```
      - `INSERT` throws an error because column `new_value` does not exist in the target table.
      - The table schema is changed to `(key, old_value, new_value)`. New records are inserted with the specified `key`, `new_value`, and `NULL` for the `old_value`. Existing records have `NULL` entered for `new_value` leaving `old_value` unchanged. See note [(1)](#1).

<a id="arrays-of-structs-considerations"></a>

## Special considerations for schemas that contain arrays of structs

Delta `MERGE INTO` supports resolving struct fields by name and evolving schemas for arrays of structs. With schema evolution enabled, target table schemas will evolve for arrays of structs, which also works with any nested structs inside of arrays.

.. note::
  In Delta 2.3 and above, struct fields present in the source table can be specified by name in insert or update commands. In Delta 2.2 and below, only `INSERT *` or `UPDATE SET *` commands can be used for schema evolution with merge.

Here are a few examples of the effects of merge operations with and without schema evolution for arrays of structs.

.. list-table::
    :header-rows: 1

    * - Source schema
      - Target schema
      - Behavior without schema evolution (default)
      - Behavior with schema evolution

    * - array<struct<b: string, a: string>>
      - array<struct<a: int, b: int>>
      - The table schema remains unchanged. Columns will be resolved by name and updated or inserted.
      - The table schema remains unchanged. Columns will be resolved by name and updated or inserted.

    * - array<struct<a: int, c: string, d: string>>
      - array<struct<a: string, b: string>>
      - `update` and `insert` throw errors because `c` and `d` do not exist in the target table.
      - The table schema is changed to array<struct<a: string, b: string, c: string, d: string>>. `c` and `d` are inserted as `NULL` for existing entries in the target table. `update` and `insert` fill entries in the source table with `a` casted to string and `b` as `NULL`.

    * - array<struct<a: string, b: struct<c: string, d: string>>>
      - array<struct<a: string, b: struct<c: string>>>
      - `update` and `insert` throw errors because `d` does not exist in the target table.
      - The target table schema is changed to array<struct<a: string, b: struct<c: string, d: string>>>. `d` is inserted as `NULL` for existing entries in the target table.

### Performance tuning

You can reduce the time taken by merge using the following approaches:

- **Reduce the search space for matches**: By default, the `merge` operation searches the entire Delta table to find matches in the source table. One way to speed up `merge` is to reduce the search space by adding known constraints in the match condition. For example, suppose you have a table that is partitioned by `country` and `date` and you want to use `merge` to update information for the last day and a specific country. Adding the condition

  ```sql
  events.date = current_date() AND events.country = 'USA'
  ```

  will make the query faster as it looks for matches only in the relevant partitions. Furthermore, it will also reduce the chances of conflicts with other concurrent operations. See [_](concurrency-control.md) for more details.

- **Compact files**: If the data is stored in many small files, reading the data to search for matches can become slow. You can compact small files into larger files to improve read throughput. See [_](best-practices.md#compact-files) for details.

- **Control the shuffle partitions for writes**: The `merge` operation shuffles data multiple times to compute and write the updated data. The number of tasks used to shuffle is controlled by the Spark session configuration `spark.sql.shuffle.partitions`. Setting this parameter not only controls the parallelism but also determines the number of output files. Increasing the value increases parallelism but also generates a larger number of smaller data files.

- **Repartition output data before write**: For partitioned tables, `merge` can produce a much larger number of small files than the number of shuffle partitions. This is because every shuffle task can write multiple files in multiple partitions, and can become a performance bottleneck. In many cases, it helps to repartition the output data by the table's partition columns before writing it. You enable this by setting the Spark session configuration `spark.databricks.delta.merge.repartitionBeforeWrite.enabled` to `true`.

## Merge examples

Here are a few examples on how to use `merge` in different scenarios.

.. contents:: In this section:
  :local:
  :depth: 1

<a id="merge-in-dedup"></a>

### Data deduplication when writing into Delta tables

A common ETL use case is to collect logs into Delta table by appending them to a table. However, often the sources can generate duplicate log records and downstream deduplication steps are needed to take care of them. With `merge`, you can avoid inserting the duplicate records.

.. code-language-tabs::

  ```sql
  MERGE INTO logs
  USING newDedupedLogs
  ON logs.uniqueId = newDedupedLogs.uniqueId
  WHEN NOT MATCHED
    THEN INSERT *
  ```

  ```python
  deltaTable.alias("logs").merge(
      newDedupedLogs.alias("newDedupedLogs"),
      "logs.uniqueId = newDedupedLogs.uniqueId") \
    .whenNotMatchedInsertAll() \
    .execute()
  ```

  ```scala
  deltaTable
    .as("logs")
    .merge(
      newDedupedLogs.as("newDedupedLogs"),
      "logs.uniqueId = newDedupedLogs.uniqueId")
    .whenNotMatched()
    .insertAll()
    .execute()
  ```

  ```java
  deltaTable
    .as("logs")
    .merge(
      newDedupedLogs.as("newDedupedLogs"),
      "logs.uniqueId = newDedupedLogs.uniqueId")
    .whenNotMatched()
    .insertAll()
    .execute();
  ```


.. note::
  The dataset containing the new logs needs to be deduplicated within itself. By the SQL semantics of merge, it matches and deduplicates the new data with the existing data in the table, but if there is duplicate data within the new dataset, it is inserted. Hence, deduplicate the new data before merging into the table.

If you know that you may get duplicate records only for a few days, you can optimized your query further by partitioning the table by date, and then specifying the date range of the target table to match on.

.. code-language-tabs::

  ```sql
  MERGE INTO logs
  USING newDedupedLogs
  ON logs.uniqueId = newDedupedLogs.uniqueId AND logs.date > current_date() - INTERVAL 7 DAYS
  WHEN NOT MATCHED AND newDedupedLogs.date > current_date() - INTERVAL 7 DAYS
    THEN INSERT *
  ```

  ```python
  deltaTable.alias("logs").merge(
      newDedupedLogs.alias("newDedupedLogs"),
      "logs.uniqueId = newDedupedLogs.uniqueId AND logs.date > current_date() - INTERVAL 7 DAYS") \
    .whenNotMatchedInsertAll("newDedupedLogs.date > current_date() - INTERVAL 7 DAYS") \
    .execute()
  ```

  ```scala
  deltaTable.as("logs").merge(
      newDedupedLogs.as("newDedupedLogs"),
      "logs.uniqueId = newDedupedLogs.uniqueId AND logs.date > current_date() - INTERVAL 7 DAYS")
    .whenNotMatched("newDedupedLogs.date > current_date() - INTERVAL 7 DAYS")
    .insertAll()
    .execute()
  ```

  ```java
  deltaTable.as("logs").merge(
      newDedupedLogs.as("newDedupedLogs"),
      "logs.uniqueId = newDedupedLogs.uniqueId AND logs.date > current_date() - INTERVAL 7 DAYS")
    .whenNotMatched("newDedupedLogs.date > current_date() - INTERVAL 7 DAYS")
    .insertAll()
    .execute();
  ```

This is more efficient than the previous command as it looks for duplicates only in the
last 7 days of logs, not the entire table. Furthermore, you can use this insert-only merge with Structured Streaming to perform continuous deduplication of the logs.

- In a streaming query, you can use merge operation in `foreachBatch` to continuously write any streaming data to a Delta table with deduplication. See the following [streaming example](#merge-in-streaming) for more information on `foreachBatch`.

- In another streaming query, you can continuously read deduplicated data from this Delta table. This is possible because an insert-only merge only appends new data to the Delta table.

<a id="merge-in-scd-type-2"></a>

### Slowly changing data (SCD) Type 2 operation into Delta tables

Another common operation is SCD Type 2, which maintains history of all changes made to each key in a dimensional table. Such operations require updating existing rows to mark previous values of keys as old, and the inserting the new rows as the latest values. Given a source table with updates and the target table with the dimensional data, SCD Type 2 can be expressed with `merge`.

Here is a concrete example of maintaining the history of addresses for a customer along with the active date range of each address. When a customer's address needs to be updated, you have to mark the previous address as not the current one, update its active date range, and add the new address as the current one.


.. code-language-tabs::

  ```scala
  val customersTable: DeltaTable = ...   // table with schema (customerId, address, current, effectiveDate, endDate)

  val updatesDF: DataFrame = ...          // DataFrame with schema (customerId, address, effectiveDate)

  // Rows to INSERT new addresses of existing customers
  val newAddressesToInsert = updatesDF
    .as("updates")
    .join(customersTable.toDF.as("customers"), "customerid")
    .where("customers.current = true AND updates.address <> customers.address")

  // Stage the update by unioning two sets of rows
  // 1. Rows that will be inserted in the whenNotMatched clause
  // 2. Rows that will either update the current addresses of existing customers or insert the new addresses of new customers
  val stagedUpdates = newAddressesToInsert
    .selectExpr("NULL as mergeKey", "updates.*")   // Rows for 1.
    .union(
      updatesDF.selectExpr("updates.customerId as mergeKey", "*")  // Rows for 2.
    )

  // Apply SCD Type 2 operation using merge
  customersTable
    .as("customers")
    .merge(
      stagedUpdates.as("staged_updates"),
      "customers.customerId = mergeKey")
    .whenMatched("customers.current = true AND customers.address <> staged_updates.address")
    .updateExpr(Map(                                      // Set current to false and endDate to source's effective date.
      "current" -> "false",
      "endDate" -> "staged_updates.effectiveDate"))
    .whenNotMatched()
    .insertExpr(Map(
      "customerid" -> "staged_updates.customerId",
      "address" -> "staged_updates.address",
      "current" -> "true",
      "effectiveDate" -> "staged_updates.effectiveDate",  // Set current to true along with the new address and its effective date.
      "endDate" -> "null"))
    .execute()
  ```

  ```python
  customersTable = ...  # DeltaTable with schema (customerId, address, current, effectiveDate, endDate)

  updatesDF = ...       # DataFrame with schema (customerId, address, effectiveDate)

  # Rows to INSERT new addresses of existing customers
  newAddressesToInsert = updatesDF \
    .alias("updates") \
    .join(customersTable.toDF().alias("customers"), "customerid") \
    .where("customers.current = true AND updates.address <> customers.address")

  # Stage the update by unioning two sets of rows
  # 1. Rows that will be inserted in the whenNotMatched clause
  # 2. Rows that will either update the current addresses of existing customers or insert the new addresses of new customers
  stagedUpdates = (
    newAddressesToInsert
    .selectExpr("NULL as mergeKey", "updates.*")   # Rows for 1
    .union(updatesDF.selectExpr("updates.customerId as mergeKey", "*"))  # Rows for 2.
  )

  # Apply SCD Type 2 operation using merge
  customersTable.alias("customers").merge(
    stagedUpdates.alias("staged_updates"),
    "customers.customerId = mergeKey") \
  .whenMatchedUpdate(
    condition = "customers.current = true AND customers.address <> staged_updates.address",
    set = {                                      # Set current to false and endDate to source's effective date.
      "current": "false",
      "endDate": "staged_updates.effectiveDate"
    }
  ).whenNotMatchedInsert(
    values = {
      "customerid": "staged_updates.customerId",
      "address": "staged_updates.address",
      "current": "true",
      "effectiveDate": "staged_updates.effectiveDate",  # Set current to true along with the new address and its effective date.
      "endDate": "null"
    }
  ).execute()
  ```

<a id="merge-in-cdc"></a>

### Write change data into a Delta table

Similar to SCD, another common use case, often called change data capture (CDC), is to apply
all data changes generated from an external database into a Delta table. In other words, a set
of updates, deletes, and inserts applied to an external table needs to be applied to a Delta table.
You can do this using `merge` as follows.


.. code-language-tabs::

  ```scala
  val deltaTable: DeltaTable = ... // DeltaTable with schema (key, value)

  // DataFrame with changes having following columns
  // - key: key of the change
  // - time: time of change for ordering between changes (can replaced by other ordering id)
  // - newValue: updated or inserted value if key was not deleted
  // - deleted: true if the key was deleted, false if the key was inserted or updated
  val changesDF: DataFrame = ...

  // Find the latest change for each key based on the timestamp
  // Note: For nested structs, max on struct is computed as
  // max on first struct field, if equal fall back to second fields, and so on.
  val latestChangeForEachKey = changesDF
    .selectExpr("key", "struct(time, newValue, deleted) as otherCols" )
    .groupBy("key")
    .agg(max("otherCols").as("latest"))
    .selectExpr("key", "latest.*")

  deltaTable.as("t")
    .merge(
      latestChangeForEachKey.as("s"),
      "s.key = t.key")
    .whenMatched("s.deleted = true")
    .delete()
    .whenMatched()
    .updateExpr(Map("key" -> "s.key", "value" -> "s.newValue"))
    .whenNotMatched("s.deleted = false")
    .insertExpr(Map("key" -> "s.key", "value" -> "s.newValue"))
    .execute()
  ```

  ```python
  deltaTable = ... # DeltaTable with schema (key, value)

  # DataFrame with changes having following columns
  # - key: key of the change
  # - time: time of change for ordering between changes (can replaced by other ordering id)
  # - newValue: updated or inserted value if key was not deleted
  # - deleted: true if the key was deleted, false if the key was inserted or updated
  changesDF = spark.table("changes")

  # Find the latest change for each key based on the timestamp
  # Note: For nested structs, max on struct is computed as
  # max on first struct field, if equal fall back to second fields, and so on.
  latestChangeForEachKey = changesDF \
    .selectExpr("key", "struct(time, newValue, deleted) as otherCols") \
    .groupBy("key") \
    .agg(max("otherCols").alias("latest")) \
    .select("key", "latest.*") \

  deltaTable.alias("t").merge(
      latestChangeForEachKey.alias("s"),
      "s.key = t.key") \
    .whenMatchedDelete(condition = "s.deleted = true") \
    .whenMatchedUpdate(set = {
      "key": "s.key",
      "value": "s.newValue"
    }) \
    .whenNotMatchedInsert(
      condition = "s.deleted = false",
      values = {
        "key": "s.key",
        "value": "s.newValue"
      }
    ).execute()
  ```

<a id="merge-in-streaming"></a>

### Upsert from streaming queries using `foreachBatch`

You can use a combination of `merge` and `foreachBatch` (see [foreachbatch](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch) for more information) to write complex upserts from a streaming query into a Delta table. For example:

- **Write streaming aggregates in Update Mode**: This is much more efficient than Complete Mode.


.. code-language-tabs::

  ```scala
  import io.delta.tables.*

  val deltaTable = DeltaTable.forPath(spark, "/data/aggregates")

  // Function to upsert microBatchOutputDF into Delta table using merge
  def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long) {
    deltaTable.as("t")
      .merge(
        microBatchOutputDF.as("s"),
        "s.key = t.key")
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()
  }

  // Write the output of a streaming aggregation query into Delta table
  streamingAggregatesDF.writeStream
    .format("delta")
    .foreachBatch(upsertToDelta _)
    .outputMode("update")
    .start()
  ```

  ```python
  from delta.tables import *

  deltaTable = DeltaTable.forPath(spark, "/data/aggregates")

  # Function to upsert microBatchOutputDF into Delta table using merge
  def upsertToDelta(microBatchOutputDF, batchId):
    deltaTable.alias("t").merge(
        microBatchOutputDF.alias("s"),
        "s.key = t.key") \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()
  }

  # Write the output of a streaming aggregation query into Delta table
  streamingAggregatesDF.writeStream \
    .format("delta") \
    .foreachBatch(upsertToDelta) \
    .outputMode("update") \
    .start()
  ```

- **Write a stream of database changes into a Delta table**: The [merge query for writing change data](#merge-in-cdc) can be used in `foreachBatch` to continuously apply a stream of changes to a Delta table.

- **Write a stream data into Delta table with deduplication**: The [insert-only merge query for deduplication](#merge-in-dedup) can be used in `foreachBatch` to continuously write data (with duplicates) to a Delta table with automatic deduplication.

.. note::

  - Make sure that your `merge` statement inside `foreachBatch` is idempotent as restarts
    of the streaming query can apply the operation on the same batch of data multiple times.
  - When `merge` is used in `foreachBatch`, the input data rate of the streaming query
    (reported through `StreamingQueryProgress` and visible in the notebook rate graph) may be reported
    as a multiple of the actual rate at which data is generated at the source. This is because `merge` reads the input data multiple times causing the input metrics to be multiplied. If this is a bottleneck, you can cache the batch DataFrame before `merge` and then uncache it after `merge`.

.. include:: /shared/replacements.md
