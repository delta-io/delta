# Column Updates
**Associated GitHub issue for discussions: https://github.com/delta-io/delta/issues/7414**

This protocol change introduces the Column Updates feature, which allows a writer to
replace selected columns of a data file without rewriting the other columns in that file.

The writer stores the replacement values in a Parquet column file. The column file
has one row for each physical row in its base data file. Readers combine the base
data file and its column files by physical row position.

--------

# Changes to existing sections

## Add File and Remove File

> ***In [Add File and Remove File](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file), replace the two paragraphs starting with "Every logical file" and ending with "`(path, NULL)` instead" with the following text.***

Every _logical file_ of the table is represented by a path to a data file, combined with an optional Deletion Vector (DV) and zero or more column files. A DV identifies which rows of the data file are no longer in the table. A column file supplies current values for a set of columns in the data file. Deletion Vectors and column files are optional features. See [Deletion Vectors](#deletion-vectors) and [Column Updates](#column-updates) for details.

When an `add` action is encountered for a logical file that is already present in the table, statistics and other information from the latest version should replace that from any previous version.
The primary key for the entry of a logical file in the set of files is a tuple of the data file's `path`, a unique id describing the DV, and a unique id describing the column file set. If no DV is part of this logical file, then the DV part of the primary key is `NULL`. If no column files are part of this logical file, then the column file set part of the primary key is `NULL`.

> ***In the same section, replace the paragraph starting with "In the following statements" and the five bullets that follow it with the following text.***

In the following statements, `dvId` refers to either the unique ID of a specific Deletion Vector (`deletionVector.uniqueId`) or to `NULL`, indicating that no rows are invalidated. `columnFileSetId` refers to either the identity of a specific column file set or to `NULL`, indicating that there are no associated column files.

Since actions within a given Delta commit are not guaranteed to be applied in order, a **valid** version is restricted to contain at most one file action *of the same type* (i.e. `add`/`remove`) for any one combination of `path`, `dvId` and `columnFileSetId`. Moreover, for simplicity, it is required that there is at most one file action of the same type for any `path` (regardless of `dvId` and `columnFileSetId`).
That means specifically that for any commit…

- it is **legal** for the same `path` to occur in an `add` action and a `remove` action, but with two different `dvId`s or `columnFileSetId`s.
- it is **legal** for the same `path` to be added and/or removed and also occur in a `cdc` action.
- it is **illegal** for the same `path` to occur twice within the set of `add` actions or within the set of `remove` actions, regardless of its `dvId` or `columnFileSetId`.
- it is **illegal** for a `path` to occur in an `add` action that already occurs with a different `dvId` or `columnFileSetId` in the list of `add` actions from the snapshot of the version immediately preceding the commit, unless the commit also contains a `remove` for the later combination.
- it is **legal** to commit an existing `path`, `dvId` and `columnFileSetId` combination again (this allows metadata updates).

> ***Add the following row to the schema of the `add` action, after `clusteringProvider`.***

Field Name | Data Type | Description | optional/required
-|-|-|-
columnFiles | Array[[ColumnFileDescriptor Struct](#column-file-descriptor-struct)] | The column files associated with this data file. See also [Column Updates](#column-updates). | optional

> ***Add the following row to the schema of the `remove` action, after `defaultRowCommitVersion`.***

Field Name | Data Type | Description | optional/required
-|-|-|-
columnFiles | Array[[ColumnFileDescriptor Struct](#column-file-descriptor-struct)] | The column files associated with the logical file being removed. See also [Column Updates](#column-updates). | optional

## Action Reconciliation

> ***In [Action Reconciliation](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#action-reconciliation), replace the two collection bullets for `add` and `remove` actions with the following bullets.***

- A collection of `add` actions with unique `path` keys, corresponding to the newest `(path, deletionVector.uniqueId, columnFileSetId)` tuple encountered for each path.
- A collection of `remove` actions with unique `(path, deletionVector.uniqueId, columnFileSetId)` keys. The intersection of the primary keys in the `add` collection and `remove` collection must be empty. That means a logical file cannot exist in both the `remove` and `add` collections at the same time; however, the same *data file* can exist with *different* DVs in the `remove` collection, as logically they represent different content. The `remove` actions act as _tombstones_, and only exist for the benefit of the VACUUM command. Snapshot reads only return `add` actions on the read path.

> ***In the reconciliation rules in the same section, replace the bullet starting with "Logical files in a table" with the following bullet.***

- Logical files in a table are identified by their `(path, deletionVector.uniqueId, columnFileSetId)` primary key. File actions (`add` or `remove`) reference logical files, and a log can contain any number of references to a single file.

--------

> ***Add the following section after [Deletion Vectors](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors) and before Catalog-managed Tables.***

# Column Updates

Column Updates allow a logical file to obtain selected column values from one or more Parquet column files. The feature name is `columnUpdates`. This is a reader-writer feature. Column Updates require Column Mapping to be enabled as Column Updates apply to physical columns, not logical column names.

To support this feature:

- The table must be on Reader Version 3 and Writer Version 7.
- The feature `columnUpdates` must exist in the table protocol's `readerFeatures` and `writerFeatures`.
- The feature `columnMapping` must exist in the table protocol's `readerFeatures` and `writerFeatures`.
- The table property `delta.columnMapping.mode` must be set to `name` or `id`.

Column Updates store values in [Column Files](#column-file-format) that are tracked in metadata
using [Column File Descriptors](#column-file-descriptor-struct).

## Column File Format

A column file is a Parquet file associated with exactly one base data file. Its columns are `_pos`, `_last_updated_sequence_number`, and a subset of columns of the base file with nullability on top (i.e. `INTEGER NOT NULL`/`Int` in the base file becomes `INTEGER`/`Option[Int]` in the column file). It has one row for each physical row in the base file.

`_pos` is the physical offset of the corresponding row in the base file.

`_last_updated_sequence_number` stores row lineage information for Iceberg V4 change detection. In Delta, it contains either the commit version of the update that last changed this row, or `NULL` to indicate that the most recent update changed this row. The name mentions sequence numbers only for Iceberg compatibility, in Delta it still stores the Delta commit version.

The rest is value columns that represent the current values for the associated base file columns. Values associated with base file rows that are already deleted by a DV might either contain stale values (so that we don't need to overwrite the column file with each DV update) or `NULL`s (so that we don't need to read the old values when writing new copy-on-write column files).

For examples of column files see [Column Updates examples](#column-updates-examples).

## Column File Descriptor Struct

The `ColumnFileDescriptor` struct has the following schema:

Field Name | Data Type | Description | optional/required
-|-|-|-
fieldIds | Array[Integer] | The Column Mapping IDs of the table fields supplied by this column file. | required
path | String | A relative path to a column file from the root of the table, or an absolute path to the column file. The path uses the same URI encoding as `add.path`. | required
sizeInBytes | Long | The size of the column file in bytes. | required

`fieldIds` must not be empty and must not contain duplicates. A field ID must occur in at most one `ColumnFileDescriptor` within a single `add`. At the moment, only top-level fields are supported for Column Updates.

`_pos` does not get a field id and is not included in `fieldIds` as it is a metadata column that is always present. `_last_updated_sequence_number` does get a field id (2147483539) and might be included to indicate that the associated column file is the most recently written one. This implies that for file actions that contain any amount of column files, there should be exactly one `ColumnFileDescriptor` that contains the field id for `_last_updated_sequence_number`.

## Column File Set Identity

The identity of a column file set is the sorted list of each descriptor's `path` and sorted `fieldIds`. These are the only fields that affect the logical data.

## Reader Requirements for Column Updates

During a read, if a field is present in `columnFiles[].fieldIds`, the reader must consider the corresponding values for that column in the base file as invalid and read them from the column file instead.

The row-group boundaries of a column file and its base data file may differ. A reader must align column file rows with base data file rows using the `_pos` column. This version of the spec disallows row counts being different between column files and the base data file, so aligning using `_pos` is equivalent to aligning positionally.

## Writer Requirements for Column Updates

During a write that uses the Column Updates feature, the writer must:

1. copy over all previous `ColumnFileDescriptor`s;
2. for each field id that contains changes in the current write, remove that field id from all `ColumnFileDescriptor`s;
3. remove field id for `_last_updated_sequence_number` from any `ColumnFileDescriptor`s that contain it;
4. add a new `ColumnFileDescriptor` that contains the newly written column file path, along with all field ids that contain changes in the current write and field id for `_last_updated_sequence_number`;
5. remove all `ColumnFileDescriptor`s that contain no associated field ids as a result of step (2).

## Column File Cleanup

Column files are table data. VACUUM must preserve all column files referenced by a retained file action. A column file may be deleted only when no retained file action references its path and the retention period has passed.

The `columnUpdates` feature must not be removed while any retained table version references a column file.

## Column Updates Examples

This table shows the Parquet files and log actions after each statement, assuming every `UPDATE` uses the Column Updates feature. The example assumes that the column `foo` has Column Mapping ID 7, and column `bar` has ID 8. The table abbreviates `_last_updated_sequence_number` to `_lusn`, and `add`/`remove` actions only contain relevant fields.

<table>
<tr>
<td> <b> Statement </b> </td> <td> <b> Parquet </b> </td> <td> <b> Log </b> </td>
</tr>
<tr>
<td>

```
CREATE TABLE t (
  key STRING,
  foo INTEGER NOT NULL,
  bar INTEGER
)

INSERT INTO t
  VALUES
    ("a", 1, 1)
    ("b", 2, 2)
```

</td>
<td>

`base.parquet`:

| key | foo (7) | bar (8) |
|-|-|-|
| a | 1 | 1 |
| b | 2 | 2 |

</td>
<td>

```
{
  "add": {
    "path": "base.parquet"
  }
}
```

</td>
</tr>
<tr>
<td>

```
UPDATE t SET
  foo = 200,
  bar = 100
```

</td>
<td>

`base.parquet`:

| key | foo (7) | bar (8) |
|-|-|-|
| a | 1 | 1 |
| b | 2 | 2 |

`column-1.parquet`:

| _pos | _lusn | foo (7) | bar (8) |
|-|-|-|-|
| 0 | NULL | 200 | 100 |
| 1 | NULL | 200 | 100 |

</td>
<td>

```
{
  "remove": {
    "path": "base.parquet"
  },
  "add": {
    "path": "base.parquet",
    "columnFiles": [
      {
        "path": "column-1.parquet",
        "fieldIds": [7, 8, 2147483539]
      }
    ]
  }
}
```

</td>
</tr>
<tr>
<td>

```
UPDATE t SET
  foo = 500
WHERE key = 'a'
```

</td>
<td>

`base.parquet`:

| key | foo (7) | bar (8) |
|-|-|-|
| a | 1 | 1 |
| b | 2 | 2 |

`column-1.parquet`:

| _pos | _lusn | foo (7) | bar (8) |
|-|-|-|-|
| 0 | NULL | 200 | 100 |
| 1 | NULL | 200 | 100 |

`column-2.parquet`:

| _pos | _lusn | foo (7) |
|-|-|-|
| 0 | NULL | 500 |
| 1 | 2 | 200 |

</td>
<td>

```
{
  "remove": {
    "path": "base.parquet",
    "columnFiles": [
      {
        "path": "column-1.parquet",
        "fieldIds": [7, 8, 2147483539]
      }
    ]
  },
  "add": {
    "path": "base.parquet",
    "columnFiles": [
      {
        "path": "column-1.parquet",
        "fieldIds": [8]
      },
      {
        "path": "column-2.parquet",
        "fieldIds": [7, 2147483539]
      }
    ]
  }
}
```

</td>
</tr>
</table>

--------

## Valid Feature Names in Table Features

> ***Add the following row after Deletion Vectors in [Valid Feature Names in Table Features](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features).***

Feature | Name | Readers or Writers?
-|-|-
[Column Updates](#column-updates) | `columnUpdates` | Readers and writers
