---
description: Learn how to set up an integration to enable you to read Delta tables from AWS Athena.
---

# AWS Athena Delta Connector
Since Athena [version 3](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html), Athena natively supports reading <Delta> tables. For details on using the native Delta Lake connector, see [Querying Delta Lake tables](https://docs.aws.amazon.com/athena/latest/ug/delta-lake-tables.html). For Athena versions lower than [version 3](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html), you can use the manifest-based approach detailed in [_](/presto-integration.md).

.. <Delta> replace:: Delta Lake