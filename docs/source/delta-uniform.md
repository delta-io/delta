---
description: Configure Delta tables to be read as Iceberg tables using UniForm.
---

# Universal Format (UniForm)

Delta Universal Format (UniForm) allows you to read Delta tables with Iceberg clients.

UniForm takes advantage of the fact that both <Delta> and Iceberg consist of Parquet data files and a metadata layer. UniForm automatically generates Iceberg metadata asynchronously, allowing Iceberg clients to read Delta tables as if they were Iceberg tables. You can expect negligible Delta write overhead when UniForm is enabled, as the Iceberg conversion and transaction occurs asynchronously after the Delta commit.

A single copy of the data files provides access to both format clients.

## Requirements

To enable UniForm, you must fulfill the following requirements:

- The table must have column mapping enabled. See [_](delta-column-mapping.md).
- The Delta table must have a `minReaderVersion` >= 2 and `minWriterVersion` >= 7.
- Writes to the table must use <Delta> 3.1 or above.
- Hive Metastore (HMS) must be configured as the catalog. See [the HMS documentation](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html) for how to configure <AS> to use Hive Metastore.

## Enable <Delta> UniForm

.. important::

  Enabling Delta UniForm sets the Delta table feature `IcebergCompatV2`, a write protocol feature. Only clients that support this table feature can write to enabled tables. You must use <Delta> 3.1 or above to write to Delta tables with this feature enabled.

The following table properties enable UniForm support for Iceberg. `iceberg` is the only valid value.

```
'delta.enableIcebergCompatV2' = 'true'
'delta.universalFormat.enabledFormats' = 'iceberg'
```


You must also enable column mapping to use UniForm. It is set automatically during table creation, as in the following example:

```sql
CREATE TABLE T(c1 INT) USING DELTA TBLPROPERTIES(
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg');
```

You can enable UniForm on an existing table using the following syntax:


```sql
REORG TABLE table_name APPLY (UPGRADE UNIFORM(ICEBERG_COMPAT_VERSION=2));
```
.. note:: This syntax requires [_](delta-column-mapping.md) to be enabled on the table prior to running on Delta 3.1. This syntax also works to upgrade from the IcbergCompatV1. It may rewrite existing files to make those Iceberg compatible, and it automatically disables and purges Deletion Vectors from the table.

.. important:: When you first enable UniForm, asynchronous metadata generation begins. This task must complete before external clients can query the table using Iceberg. See [_](#status).

.. warning:: You can turn off UniForm by unsetting the `delta.universalFormat.enabledFormats` table property. You cannot turn off column mapping once enabled, and upgrades to <Delta> reader and writer protocol versions cannot be undone.

See [_](#limitations).

## When does UniForm generate Iceberg metadata?

<Delta> triggers Iceberg metadata generation asynchronously after a <Delta> write transaction completes using the same compute that completed the Delta transaction.

Iceberg can have significantly higher write latencies than <Delta>. Delta tables with frequent commits might bundle multiple Delta commits into a single Iceberg commit.

<Delta> ensures that only one Iceberg metadata generation process is in progress at any time in a single cluster. Commits that would trigger a second concurrent Iceberg metadata generation process successfully commit to Delta, but do not trigger asynchronous Iceberg metadata generation. This prevents cascading latency for metadata generation for workloads with frequent commits (seconds to minutes between commits).

## <a id="status"></a> Check Iceberg metadata generation status

UniForm adds the following properties to Iceberg table metadata to track metadata generation status:

| Table property | Description |
| --- | --- |
| `converted_delta_version` | The latest version of the Delta table for which Iceberg metadata was successfully generated. |
| `converted_delta_timestamp` | The timestamp of the latest Delta commit for which Iceberg metadata was successfully generated. |

See documentation for your Iceberg reader client for how to review table properties outside <Delta>. For <AS>, you can see these properties using the following syntax:

```sql
SHOW TBLPROPERTIES <table-name>;
```

## Read UniForm tables as Iceberg tables in <AS>

You are able to read UniForm tables as Iceberg tables in <AS> with the following steps:
* Start <AS> with Iceberg, and connect to the Hive Metastore used by UniForm. Please refer to the [Iceberg documentation](https://iceberg.apache.org/docs/latest/spark-configuration/#catalogs) for how to run Iceberg with <AS> and connect to a Hive Metastore.
* Use the `SHOW TABLES` command to see a list of available Iceberg tables in the catalog.
* Read an Iceberg table using standard SQL such as `SELECT`. 

## Read using a metadata JSON path

Some Iceberg clients allow you to register external Iceberg tables by providing a path to versioned metadata files. Each time UniForm converts a new version of the Delta table to Iceberg, it creates a new metadata JSON file.

Clients that use metadata JSON paths for configuring Iceberg include BigQuery. Refer to documentation for the Iceberg reader client for configuration details.

<Delta> stores Iceberg metadata under the table directory, using the following pattern:

```
<table-path>/metadata/v<version-number>-uuid.metadata.json
```

## <a id="versions"></a> Delta and Iceberg table versions

Both <Delta> and Iceberg allow time travel queries using table versions or timestamps stored in table metadata.

In general, Iceberg and Delta table versions do not align by either the commit timestamp or the version ID. If you wish to verify which version of a Delta table a given version of an Iceberg table corresponds to, you can use the corresponding table properties set on the Iceberg table. See [_](#status).

## Limitations

.. warning:: UniForm is read-only from an Iceberg perspective, though this cannot yet be enforced since UniForm uses HMS as catalog. If any external writer (not <Delta>) writes to this Iceberg table, this may destroy your Delta table and cause data loss, as the Iceberg writer may perform data cleanup or garbage collection that Delta is unaware of.

The following limitations exist:

- UniForm does not work on tables with deletion vectors enabled. See [_](delta-deletion-vectors.md).
- Delta tables with UniForm enabled do not support `VOID` type.
- Iceberg clients can only read from UniForm. Writes are not supported.
- Iceberg reader clients might have individual limitations, regardless of UniForm. See documentation for your target client.

The following <Delta> features work for Delta clients when UniForm is enabled, but do not have support in Iceberg:

- Change Data Feed
- Delta Sharing

.. include:: /shared/replacements.md
