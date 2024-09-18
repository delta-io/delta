---
description: Learn how to set up an integration to enable you to read Delta tables from Presto.
---

# Presto connector
Since Presto [version 0.269](https://prestodb.io/docs/0.269/release/release-0.269.html#delta-lake-connector-changes), Presto natively supports reading <Delta> tables. For details on using the native Delta Lake connector, see [Delta Lake Connector - Presto](https://prestodb.io/docs/current/connector/deltalake.html). For Presto versions lower than [0.269](https://prestodb.io/docs/0.269/release/release-0.269.html#delta-lake-connector-changes), you can use the manifest-based approach detailed in [_](/presto-integration.md).

.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark