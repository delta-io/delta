---
description: Learn about liquid clustering in <Delta>.
orphan: 1
---

# Use liquid clustering for Delta tables

Liquid clustering improves the existing partitioning and `ZORDER` techniques by simplifying data layout decisions in order to optimize query performance. Liquid clustering provides flexibility to redefine clustering keys without rewriting existing data, allowing data layout to evolve alongside analytic needs over time.

.. note:: This feature is available in <Delta> 3.1.0 and above. This feature is in experimental support mode with [_](#limitations).

## What is liquid clustering used for?

The following are examples of scenarios that benefit from clustering:

- Tables often filtered by high cardinality columns.
- Tables with significant skew in data distribution.
- Tables that grow quickly and require maintenance and tuning effort.
- Tables with access patterns that change over time.
- Tables where a typical partition key could leave the table with too many or too few partitions.

## Enable liquid clustering

You must enable liquid clustering when first creating a table. Clustering is not compatible with partitioning or `ZORDER`. Once enabled, run `OPTIMIZE` jobs as normal to incrementally cluster data. See [_](#optimize).

To enable liquid clustering, add the `CLUSTER BY` phrase to a table creation statement, as in the examples below:

```sql
-- Create an empty table
CREATE TABLE table1(col0 int, col1 string) USING DELTA CLUSTER BY (col0);

-- Using a CTAS statement
CREATE TABLE table2 CLUSTER BY (col0)  -- specify clustering after table name, not in subquery
AS SELECT * FROM table1;
```

.. warning:: Tables created with liquid clustering enabled have Clustering and Domain metadata table feature enabled at creation and use Delta writer version 7 and reader version 3. Table protocol versions cannot be downgraded. See [_](/versioning.md).

## Choose clustering keys

Clustering keys can be defined in any order. If two columns are correlated, you only need to add one of them as a clustering key.

If you're converting an existing table, consider the following recommendations:

| Current data optimization technique | Recommendation for clustering keys |
| --- | --- |
| Hive-style partitioning | Use partition columns as clustering keys. |
| Z-order indexing | Use the `ZORDER BY` columns as clustering keys. |
| Hive-style partitioning and Z-order | Use both partition columns and `ZORDER BY` columns as clustering keys. |
| Generated columns to reduce cardinality (for example, date for a timestamp) | Use the original column as a clustering key, and don't create a generated column. |

## Write data to a clustered table

You must use a Delta writer client that supports Clustering and Domain metadata table feature.

## <a id="optimize"></a> How to trigger clustering

Use the `OPTIMIZE` command on your table, as in the following example:

```sql
OPTIMIZE table_name;
```

<!-- Commenting out for now as it's not supported yet.
Liquid clustering is incremental, meaning that data is only rewritten as necessary to accommodate data that needs to be clustered. Data files with clustering keys that do not match data to be clustered are not rewritten. -->

## Read data from a clustered table

You can read data in a clustered table using any <Delta> client. For best query results, include clustering keys in your query filters, as in the following example:

```sql
SELECT * FROM table_name WHERE cluster_key_column_name = "some_value";
```

<!-- Commenting out for now as it's not supported yet.
## Change clustering keys

You can change clustering keys for a table at any time by running an `ALTER TABLE` command, as in the following example:

```sql
ALTER TABLE table_name CLUSTER BY (new_column1, new_column2);
```

When you change clustering keys, subsequent `OPTIMIZE` and write operations use the new clustering approach, but existing data is not rewritten.

You can also turn off clustering by setting the keys to `NONE`, as in the following example:

```sql
ALTER TABLE table_name CLUSTER BY NONE;
```

Setting cluster keys to `NONE` does not rewrite data that has already been clustered, but prevents future `OPTIMIZE` operations from using clustering keys.

## See how table is clustered

You can use `DESCRIBE DETAIL` commands to see the clustering keys for a table, as in the following examples:

```sql
DESCRIBE DETAIL table_name;
``` -->

## Limitations

The following limitations exist:

- In <Delta> 3.1, users needs to enable the feature flag `spark.databricks.delta.clusteredTable.enableClusteringTablePreview` to use liquid clustering. The following features are not supported in this preview:
  - Incremental clustering
  - ALTER TABLE ... CLUSTER BY
  - DESCRIBE DETAIL
- You can only specify columns with statistics collected for clustering keys. By default, the first 32 columns in a Delta table have statistics collected.
- You can specify up to 4 columns as clustering keys.

.. include:: /shared/replacements.md
