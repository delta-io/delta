---
description: Learn how to set up an integration to enable you to read Delta tables from Trino.
---

# Trino
Since Trino [version 373](https://trino.io/docs/current/release/release-373.html), Trino natively supports reading and writing the <Delta> tables. For details on using the native Delta Lake connector, see [Delta Lake Connector - Trino](https://trino.io/docs/current/connector/delta-lake.html). For Trino versions lower than [version 373](https://trino.io/docs/current/release/release-373.html), you can use the manifest-based approach detailed in [_](/presto-integration.md).

.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark