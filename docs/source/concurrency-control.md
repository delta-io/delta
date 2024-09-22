---
description: Learn about the ACID transaction guarantees between reads and writes provided by <Delta>.
---

# Concurrency control

<Delta> provides ACID transaction guarantees between reads and writes. This means that:

* For supported [storage systems](delta-storage.md), multiple writers across multiple clusters can simultaneously modify a table partition and see a consistent snapshot view of the table and there will be a serial order for these writes.
* Readers continue to see a consistent snapshot view of the table that the <AS> job started with, even when a table is modified during a job.


.. contents:: In this article:
  :local:
  :depth: 1

## Optimistic concurrency control

<Delta> uses [optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control)
to provide transactional guarantees between writes. Under this mechanism, writes operate in three stages:

1. **Read**: Reads (if needed) the latest available version of the table to identify which files need to be modified (that is, rewritten).
#. **Write**: Stages all the changes by writing new data files.
#. **Validate and commit**: Before committing the changes, checks whether the proposed changes conflict with any other changes that may have been concurrently committed since the snapshot that was read. If there are no conflicts, all the staged changes are committed as a new versioned snapshot, and the write operation succeeds. However, if there are conflicts, the write operation fails with a concurrent modification exception rather than corrupting the table as would happen with the write operation on a Parquet table.

## Write conflicts

The following table describes which pairs of write operations can conflict. Compaction refers to [file compaction operation](best-practices.md#compact-files) written with the option `dataChange` set to `false`.

.. list-table::
    :header-rows: 1

    * -
      - INSERT
      - UPDATE, DELETE, MERGE INTO
      - COMPACTION
    * - **INSERT**
      - Cannot conflict
      -
      -
    * - **UPDATE, DELETE, MERGE INTO**
      - Can conflict
      - Can conflict
      -
    * - **COMPACTION**
      - Cannot conflict
      - Can conflict
      - Can conflict


## Avoid conflicts using partitioning and disjoint command conditions

In all cases marked "can conflict", whether the two operations will conflict depends on whether they operate on the same set of files. You can make the two sets of files disjoint by partitioning the table by the same columns as those used in the conditions of the operations. For example, the two commands `UPDATE table WHERE date > '2010-01-01' ...` and `DELETE table WHERE date < '2010-01-01'` will conflict if the table is not partitioned by date, as both can attempt to modify the same set of files. Partitioning the table by `date` will avoid the conflict. Hence, partitioning a table according to the conditions commonly used on the command can reduce conflicts significantly. However, partitioning a table by a column that has high cardinality can lead to other performance issues due to large number of subdirectories.

## Conflict exceptions

When a transaction conflict occurs, you will observe one of the following exceptions:

.. contents::
  :local:
  :depth: 1

### ConcurrentAppendException

This exception occurs when a concurrent operation adds files in the same partition (or anywhere in an unpartitioned table) that your operation reads. The file additions can be caused by `INSERT`, `DELETE`, `UPDATE`, or `MERGE` operations.

This exception is often thrown during concurrent `DELETE`, `UPDATE`, or `MERGE` operations. While the concurrent operations may be physically updating different partition directories, one of them may read the same partition that the other one concurrently updates, thus causing a conflict. You can avoid this by making the separation explicit in the operation condition. Consider the following example.

```scala
// Target 'deltaTable' is partitioned by date and country
deltaTable.as("t").merge(
    source.as("s"),
    "s.user_id = t.user_id AND s.date = t.date AND s.country = t.country")
  .whenMatched().updateAll()
  .whenNotMatched().insertAll()
  .execute()
```

Suppose you run the above code concurrently for different dates or countries. Since each job is working on an independent partition on the target Delta table, you don't expect any conflicts. However, the condition is not explicit enough and can scan the entire table and can conflict with concurrent operations updating any other partitions. Instead, you can rewrite your statement to add specific date and country to the merge condition, as shown in the following example.

```scala
// Target 'deltaTable' is partitioned by date and country
deltaTable.as("t").merge(
    source.as("s"),
    "s.user_id = t.user_id AND s.date = t.date AND s.country = t.country AND t.date = '" + <date> + "' AND t.country = '" + <country> + "'")
  .whenMatched().updateAll()
  .whenNotMatched().insertAll()
  .execute()
```

This operation is now safe to run concurrently on different dates and countries.

### ConcurrentDeleteReadException

This exception occurs when a concurrent operation deleted a file that your operation read. Common causes are a `DELETE`, `UPDATE`, or `MERGE` operation that rewrites files.

### ConcurrentDeleteDeleteException

This exception occurs when a concurrent operation deleted a file that your operation also deletes. This could be caused by two concurrent compaction operations rewriting the same files.

### MetadataChangedException

This exception occurs when a concurrent transaction updates the metadata of a Delta table. Common causes are `ALTER TABLE` operations or writes to your Delta table that update the schema of the table.

### ConcurrentTransactionException

If a streaming query using the same checkpoint location is started multiple times concurrently and tries to write to the Delta table at the same time. You should never have two streaming queries use the same checkpoint location and run at the same time.

### ProtocolChangedException

This exception can occur in the following cases:

- When your Delta table is upgraded to a new version. For future operations to succeed you may need to upgrade your <Delta> version.
- When multiple writers are creating or replacing a table at the same time.
- When multiple writers are writing to an empty path at the same time.


.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark
