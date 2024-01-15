---
description: Learn about deletion vectors in <Delta>.
orphan: 1
---

# What are deletion vectors?

.. note:: This feature is available in <Delta> 2.3.0 and above. This feature is in experimental support mode with [_](#limitations).

Deletion vectors are a storage optimization feature that can be enabled on <Delta> tables. By default, when a single row in a data file is deleted, the entire Parquet file containing the record must be rewritten. With deletion vectors enabled for the table, some Delta operations use deletion vectors to mark existing rows as removed without rewriting the Parquet file. Subsequent reads on the table resolve current table state by applying the deletions noted by deletion vectors to the most recent table version.

## Enable deletion vectors

<Delta> 2.4 and above leverages deletion vectors to accelerate `DELETE` operations on a supported Delta table. <Delta> 3.0 adds support for deletion vectors in `UPDATE` and <Delta> 3.1 adds support for deletion vectors in `MERGE`.

You enable support for deletion vectors on a <Delta> table by setting a <Delta> table property:

```sql
ALTER TABLE <table_name> SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
```

.. warning:: When you enable deletion vectors, the table protocol version is upgraded. Table protocol version upgrades are not reversible. After upgrading, the table will not be readable by <Delta> clients that do not support deletion vectors. See [_](versioning.md).

<a id="apply-changes"></a>

## Apply changes to Parquet data files

Deletion vectors indicate changes to rows as soft-deletes that logically modify existing Parquet data files in the <Delta> tables. These changes are applied physically when data files are rewritten, as triggered by one of the following events:

* An `UPDATE` or `MERGE` command with deletion vectors disabled is run on the table.
* An `OPTIMIZE` command is run on the table.
* `REORG TABLE ... APPLY (PURGE)` is run against the table.

`UPDATE`, `MERGE`, and `OPTIMIZE` do not have strict guarantees for resolving changes recorded in deletion vectors, and some changes recorded in deletion vectors might not be applied if target data files contain no updated records, or would not otherwise be candidates for file compaction. `REORG TABLE ... APPLY (PURGE)` rewrites all data files containing records with modifications recorded using deletion vectors. See [_](#apply-changes-with-reorg-table)

.. note:: Modified data might still exist in the old files. You can run `VACUUM` to physically delete the old files. `REORG TABLE ... APPLY (PURGE)` creates a new version of the table at the time it completes, which is the timestamp you must consider for the retention threshold for your `VACUUM` operation to fully remove deleted files.

### Apply changes with `REORG TABLE`

Reorganize a <Delta> table by rewriting files to purge soft-deleted data, such as rows marked as deleted by deletion vectors with `REORG TABLE`:

```sql
REORG TABLE events APPLY (PURGE);

 -- If you have a large amount of data and only want to purge a subset of it, you can specify an optional partition predicate using `WHERE`:
REORG TABLE events WHERE date >= '2022-01-01' APPLY (PURGE);

REORG TABLE events
  WHERE date >= current_timestamp() - INTERVAL '1' DAY
  APPLY (PURGE);
```

.. note::

  - `REORG TABLE` only rewrites files that contain soft-deleted data.
  - When resulting files of the purge are small, `REORG TABLE` will coalesce them into larger ones. See [OPTIMIZE](/optimizations-oss.html) for more info.
  - `REORG TABLE` is _idempotent_, meaning that if it is run twice on the same dataset, the second run has no effect.
  - After running `REORG TABLE`, the soft-deleted data may still exist in the old files. You can run [VACUUM](delta-utility.md#delta-vacuum) to physically delete the old files.

<a id="limitations"></a>

## Limitations

- In <Delta> 2.3, users are only allowed to _read_ Delta tables that have Deletion vectors feature supported. Write operations to the table, such as `INSERT`, `UPDATE`, `MERGE`, and `ALTER TABLE`, are explicitly blocked. [Change data feed](/delta-change-data-feed.md) reads are also blocked on tables that support Deletion vectors. 

- In <Delta> 2.4, users are allowed to _read_ and _write_ Delta tables that have Deletion vectors feature supported. `UPDATE` or `MERGE` operations may apply changes to Parquet files which contains updated or deleted rows, see [_](#apply-changes).

- In <Delta> 3.0, deletion vectors support in `DELETE` is not enabled by default.

.. include:: /shared/replacements.md
