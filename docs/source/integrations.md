---
description: Learn how to access Delta tables from external data processing engines.
---

# Access Delta tables from external data processing engines

You can access Delta tables from <AS> and [other data processing systems](https://delta.io/integrations). Here is the list of integrations that enable you to access Delta tables from external data processing engines.

## Presto to <Delta> integration
Since Presto [version 0.269](https://prestodb.io/docs/0.269/release/release-0.269.html#delta-lake-connector-changes), Presto natively supports reading <Delta> tables. For details on using the native Delta Lake connector, see [Delta Lake Connector - Presto](https://prestodb.io/docs/current/connector/deltalake.html). For Presto versions lower than [0.269](https://prestodb.io/docs/0.269/release/release-0.269.html#delta-lake-connector-changes), you can use the manifest-based approach detailed in [_](/presto-integration.md).

## Trino to <Delta>  integration
Since Trino [version 373](https://trino.io/docs/current/release/release-373.html), Trino natively supports reading and writing the <Delta> tables. For details on using the native Delta Lake connector, see [Delta Lake Connector - Trino](https://trino.io/docs/current/connector/delta-lake.html). For Trino versions lower than [version 373](https://trino.io/docs/current/release/release-373.html), you can use the manifest-based approach detailed in [_](/presto-integration.md).

## Athena to <Delta> integration
Since Athena [version 3](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html), Athena natively supports reading <Delta> tables. For details on using the native Delta Lake connector, see [Querying Delta Lake tables](https://docs.aws.amazon.com/athena/latest/ug/delta-lake-tables.html). For Athena versions lower than [version 3](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html), you can use the manifest-based approach detailed in [_](/presto-integration.md).


## Other integrations

.. toctree::
    :maxdepth: 1
    :glob:

    presto-integration
    redshift-spectrum-integration
    snowflake-integration
    bigquery-integration
    hive-integration
    flink-integration
    delta-standalone

.. include:: /shared/replacements.md
