# Clustering by expressions

**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/6736**

## Overview

Currently, Delta tables support clustering through the `clustering` writer table feature, which spells out a spec for domain metadata for clustering columns. However, the format for clustering columns present there does not allow for representing a transform on a clustering column, such as `cast()`, `date_trunc()`, `upper()`, `lower()`, or `variant_get()`.

This RFC proposes a new writer-only table feature called `clusteringExpressions`. When supported, this feature expects writers to be able to write statistics for clustering columns that have transforms defined on a base column.

## Motivation

Clustering on a transformed column is a natural evolution of clustering on a column directly, and allows for better flexibility and decoupling of clustering axis from the physical data values. For instance, if queries are likely to cast a string column to an int before doing a filter on it, it is more appropriate to cluster the table by the int cast of that column, instead of the underlying string value which will sort differently than an int cast.

Another common use-case is to break out a subfield of a variant column, and cluster on that. In order to avoid having to add this column separately to the Delta table's schema, a quicker and more flexible option is to just cluster on a `variant_get` expression on that column that breaks out the field that is likely to be queried on.

This RFC suggests adding support for clustering-by expressions, and aims to build a generic writer feature and domain metadata syntax that writers can understand what expression and column a table is clustered on. Writers can then write per-file statistics for clustering columns with transform expressions on them, following a similar requirement as clustering on a column directly.

--------

> ***New Section after Identity Columns section***

## Clustering by expressions

The Clustered-by Expressions Table feature facilitates the physical clustering of rows that share similar values on a predefined set of clustering columns, with transforms defined on them.
Clustering columns can be specified during the initial creation of a table, or they can be added later, provided that the table doesn't have partition columns.

A table is defined as a table that's clustered on an expression through the following criteria:
- When the feature `clusteringExpressions`  exists in the table `protocol`'s `writerFeatures`, and the `delta.clustering` domain metadata for that table has a non-empty `transforms` array.
  The features `domainMetadata` and `clustering` are required in the table `protocol`'s `writerFeatures` in addition to `clusteringExpressions`. 

Enablement:
- The table must be on Writer Version 7.
- The feature `clusteringExpressions` must exist in the table `protocol`'s `writerFeatures`, either during its creation or at a later stage, provided the table does not have partition columns. Dependent features `clustering` and `domainMetadata` must also be present in the `protocol`'s `writerFeatures`.
- `delta.clustering` Domain metadata has non-empty `clusteringColumns` and `transforms` arrays.

## Writer Requirements for a Clustered-by-expressions Table

When the Clustering-by expressions feature is supported (when the `writerFeatures` field of a table's `protocol` action contains `clusteringExpressions`), then:
- Writers must track clustering column names in a `domainMetadata` action with `delta.clustering` as the `domain` and a `configuration` containing all clustering column names and transforms on them, similar to [Clustered table](#clustered-table).
  If [Column Mapping](#column-mapping) is enabled, the physical column names should be used.
- Transforms must adhere to these constraints:
  - A transform is defined as a transformation function with an argument bound to a column, eg. `upper(col)` is an uppercase transformation with transform function `upper` on column `col`.
  - Transforms must be deterministic; the same input arguments into a transform must produce the same output always.
  - Transforms must be unary; they must be bound on the value of exactly one base column per transform.
  - In case of differences in SQL syntax, the signature of a transform function should adhere to Apache Spark SQL semantics as much as possible when representing it in domain metadata. This is the same expectation as with [CHECK constraints](#check-constraints); engines other than Apache Spark should translate supported transform functions to whatever's appropriate for that engine.
  - Writers can maintain allowlists of supported transform functions, and block writes to tables with clustering transform functions that are unsupported.
  - Literal arguments are represented as strings in SQL syntax, the way they would be represented if the transform were to be written out in a SQL query.
- Writers must write out [per-file statistics](#per-file-statistics) and per-column statistics for clustering columns with transforms in `add` actions, in addition to clustering columns without transforms.
  If a new column is included in the clustering columns list, it is required for all table files to have statistics for these added columns.
  - Statistics must be written in this syntax in an `add` action's per-file statistics field in the `expressionStats` field:
    ```json
      {
        "numRecords": ...,
        "tightBounds": true,
        "minValues": {...},
        "maxValues": {...},
        "expressionStats": {
          "clusteringCol_signature_23aeb9": {
            "minValues": {"dateCol": "<minValue>"},
            "maxValues": {"dateCol": "<maxValue>"}
          }
        },
      }
    ```
    In the above example, `clusteringCol_signature_23aeb9` is the expression's signature as defined in `expressionStatsColName` in clustering domian metadata (see below). The `minValues` and `maxValues` values are the minimum and maximum values observed when the matching transform function defined in clustering domain metadata is run on all records for column `dateCol` in this data file.
  - One allowed special case is where `expressionStatsColName` is missing in the domain metadata definition for a transform, the clustering column is of type `variant`, variant shredding is enabled on the table (see [Variant Shredding RFC](https://github.com/delta-io/delta/blob/master/protocol_rfcs/variant-shredding.md)), and the transform function is `variant_get` or `try_variant_get`. In this case, the writer should only guarantee that variant shredding stats are written for any shredded columns on the base clustering column; no additional expression-specific stats need to be written to the `expressionStats` field.
- Clustering columns can be repeated in the clusteringColumns list in domain metadata, as long as they have distinct transforms defined on them.
- When a clustering implementation clusters files, writers must set the name of the clustering implementation in the `clusteringProvider` field when adding `add` actions for clustered files.
  - By default, a clustering implementation must only recluster files that have the field `clusteringProvider` set to the name of the same clustering implementation, or to the names of other clustering implementations that are superseded by the current clustering implementation. In addition, a clustering implementation may cluster any files with an unset `clusteringProvider` field (i.e., unclustered files).
  - Writer is not required to cluster a specific file at any specific moment.
  - A clustering implementation is free to add additional information such as adding a new user-controlled metadata domain to keep track of its metadata.
- Writers must not define clustered and partitioned table at the same time.

The following is an example for the `domainMetadata` action definition of a table with a clustering column with a `date_trunc` transform defined on it
```json
{
  "domainMetadata": {
    "domain": "delta.clustering",
    "configuration": "{\"clusteringColumns\":[\"dateCol\", \"name\"], \"transforms\":[{\"columnIndex\": 0,\"argumentIndex\": 1,\"function\": \"date_trunc\",\"arguments\": [\"month\"],\"expressionStatsColName\": \"clusteringcol_date_date_trunc_f2c6332\"}]}",}",
    "removed": false
  }
}
```
The example above converts `configuration` field into JSON format, including escaping characters. Here's how it looks in plain JSON for better understanding.

```json
{
  "clusteringColumns": ["dateCol", "name"],
  "transforms": [
    {
      "columnIndex": 0,
      "argumentIndex": 1,
      "function": "date_trunc",
      "arguments": ["'month'"],
      "expressionStatsColName": "clusteringcol_date_date_trunc_f2c6332"
    }
  ]
}
```

The above domain metadata represents clustering on the transform function `date_trunc('month', dateCol)` where the bound column is `dateCol`, and the value in that column is being truncated to its month.

Domain metadata parameter definitions:

| Parameter | Definition |
| --------- | -----------|
| columnIndex | Index of the column in `clusteringColumns` on which this transform is defined |
| argumentIndex | Index of the argument on transform function `function` where the base column's values are bound |
| function | Unique name of the transform function in ANSI SQL or Apache Spark SQL syntax |
| arguments | Literal arguments to pass to `function` other than the bound base column |
| expressionStatsColName | Signature of this transform signature for use in the `expressionStats` field in [per-file statistics](#per-file-statistics) | 
