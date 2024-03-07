---
description: Learn about <Delta> features and resources to learn about <Delta>.
---

# Introduction

[Delta Lake](https://delta.io) is an [open source project](https://github.com/delta-io/delta) that enables building a [Lakehouse architecture](https://databricks.com/blog/2020/01/30/what-is-a-data-lakehouse.html) on top of [data lakes](https://databricks.com/discover/data-lakes/introduction). <Delta> provides [ACID transactions](concurrency-control.md), scalable metadata handling, and unifies [streaming](delta-streaming.md) and [batch](delta-batch.md) data processing on top of existing <lakes>


.. <lakes> replace:: data lakes, such as S3, ADLS, GCS, and HDFS.

Specifically, <Delta> offers:

- [ACID transactions](concurrency-control.md) on Spark: Serializable isolation levels ensure that readers never see inconsistent data.
- Scalable metadata handling: Leverages Spark distributed processing power to handle all the metadata for petabyte-scale tables with billions of files at ease.
- [Streaming](delta-streaming.md) and [batch](delta-batch.md) unification: A table in <Delta> is a batch table as well as a streaming source and sink. Streaming data ingest, batch historic backfill, interactive queries all just work out of the box.
- Schema enforcement: Automatically handles schema variations to prevent insertion of bad records during ingestion.
- [Time travel](delta-batch.md#deltatimetravel): Data versioning enables rollbacks, full historical audit trails, and reproducible machine learning experiments.
- [Upserts](delta-update.md#delta-merge) and [deletes](delta-update.md#delta-delete): Supports merge, update and delete operations to enable complex use cases like change-data-capture, slowly-changing-dimension (SCD) operations, streaming upserts, and so on.

.. include:: /shared/replacements.md
