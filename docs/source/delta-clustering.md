---
description: Learn about liquid clustering in <Delta>.
orphan: 1
---

# Use liquid clustering for Delta tables

Liquid clustering improves the existing partitioning and `ZORDER` techniques by simplifying data layout decisions in order to optimize query performance. Liquid clustering provides flexibility to redefine clustering columns without rewriting existing data, allowing data layout to evolve alongside analytic needs over time.

.. note:: This feature is available in <Delta> 3.1.0 and above. See [_](#limitations).

## What is liquid clustering used for?

The following are examples of scenarios that benefit from clustering:

- Tables often filtered by high cardinality columns.
- Tables with significant skew in data distribution.
- Tables that grow quickly and require maintenance and tuning effort.
- Tables with access patterns that change over time.
- Tables where a typical partition column could leave the table with too many or too few partitions.

## Enable liquid clustering

You must enable liquid clustering when creating a table. Clustering is not compatible with partitioning or `ZORDER`. Once enabled, run `OPTIMIZE` jobs as normal to incrementally cluster data. See [_](#optimize).

To enable liquid clustering, add the `CLUSTER BY` phrase to a table creation statement, as in the examples below:

.. note:: In <Delta> 3.2 and above, you can use DeltaTable API in Python or Scala to enable liquid clustering.

.. code-language-tabs::

  ```sql
  -- Create an empty table
  CREATE TABLE table1(col0 int, col1 string) USING DELTA CLUSTER BY (col0);

  -- Using a CTAS statement
  CREATE TABLE table2 CLUSTER BY (col0)  -- specify clustering after table name, not in subquery
  AS SELECT * FROM table1;
  ```

  ```python
  # Create an empty table
  DeltaTable.create()
    .tableName("table1")
    .addColumn("col0", dataType = "INT")
    .addColumn("col1", dataType = "STRING")
    .clusterBy("col0")
    .execute()
  ```

  ```scala
  // Create an empty table
  DeltaTable.create()
    .tableName("table1")
    .addColumn("col0", dataType = "INT")
    .addColumn("col1", dataType = "STRING")
    .clusterBy("col0")
    .execute()
  ```

.. warning:: Tables created with liquid clustering have `Clustering` and `DomainMetadata` table features enabled (both writer features) and use Delta writer version 7 and reader version 1. Table protocol versions cannot be downgraded. See [_](/versioning.md).

## Choose clustering columns

Clustering columns can be defined in any order. If two columns are correlated, you only need to add one of them as a clustering column.

If you're converting an existing table, consider the following recommendations:

| Current data optimization technique | Recommendation for clustering columns |
| --- | --- |
| Hive-style partitioning | Use partition columns as clustering columns. |
| Z-order indexing | Use the `ZORDER BY` columns as clustering columns. |
| Hive-style partitioning and Z-order | Use both partition columns and `ZORDER BY` columns as clustering columns. |
| Generated columns to reduce cardinality (for example, date for a timestamp) | Use the original column as a clustering column, and don't create a generated column. |

## Write data to a clustered table

You must use a Delta writer client that supports `Clustering` and `DomainMetadata` table features.

## <a id="optimize"></a> How to trigger clustering

Use the `OPTIMIZE` command on your table, as in the following example:

```sql
OPTIMIZE table_name;
```

Liquid clustering is incremental, meaning that data is only rewritten as necessary to accommodate data that needs to be clustered. Already clustered data files with different clustering columns are not rewritten.

## Read data from a clustered table

You can read data in a clustered table using any <Delta> client. For best query results, include clustering columns in your query filters, as in the following example:

```sql
SELECT * FROM table_name WHERE clustering_column_name = "some_value";
```

## Change clustering columns

You can change clustering columns for a table at any time by running an `ALTER TABLE` command, as in the following example:

```sql
ALTER TABLE table_name CLUSTER BY (new_column1, new_column2);
```

When you change clustering columns, subsequent `OPTIMIZE` and write operations use the new clustering approach, but existing data is not rewritten.

You can also turn off clustering by setting the columns to `NONE`, as in the following example:

```sql
ALTER TABLE table_name CLUSTER BY NONE;
```

Setting cluster columns to `NONE` does not rewrite data that has already been clustered, but prevents future `OPTIMIZE` operations from using clustering columns.

## See how table is clustered

You can use `DESCRIBE DETAIL` commands to see the clustering columns for a table, as in the following examples:

```sql
DESCRIBE DETAIL table_name;
```

## Limitations

The following limitations exist:

- You can only specify columns with statistics collected for clustering columns. By default, the first 32 columns in a Delta table have statistics collected.
- You can specify up to 4 clustering columns.

.. important::
  In <Delta> 3.1, users needs to enable the feature flag `spark.databricks.delta.clusteredTable.enableClusteringTablePreview` to use liquid clustering. The following features are not supported in this preview:
  - ZCube based incremental clustering
  - `ALTER TABLE ... CLUSTER BY` to change clustering columns
  - `DESCRIBE DETAIL` to inspect the current clustering columns
  In <Delta> 3.2, the preview flag is removed and the above features are supported.

.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark