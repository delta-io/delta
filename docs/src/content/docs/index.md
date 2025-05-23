---
title: Welcome to the Delta Lake documentation
description: Learn how to use Delta Lake
sidebar:
  label: Welcome
---

[Delta Lake](https://delta.io) is an [open source project](https://github.com/delta-incubator/delta-site) that enables building a [Lakehouse architecture](https://databricks.com/blog/2020/01/30/what-is-a-data-lakehouse.html) on top of [data lakes](https://databricks.com/discover/data-lakes/introduction). Delta Lake provides [ACID transactions](/concurrency-control/), scalable metadata handling, and unifies [streaming](/delta-streaming) and [batch](/delta-batch) data processing on top of existing data lakes, such as S3, ADLS, GCS, and HDFS.

Specifically, Delta Lake offers:

- [ACID transactions](/concurrency-control/) on Spark: Serializable isolation levels ensure that readers never see inconsistent data.
- Scalable metadata handling: Leverages Spark distributed processing power to handle all the metadata for petabyte-scale tables with billions of files at ease.
- [Streaming](/delta-streaming) and [batch](/delta-batch) unification: A table in Delta Lake is a batch table as well as a streaming source and sink. Streaming data ingest, batch historic backfill, interactive queries all just work out of the box.
- Schema enforcement: Automatically handles schema variations to prevent insertion of bad records during ingestion.
- [Time travel](/delta-batch/#query-an-older-snapshot-of-a-table-time-travel): Data versioning enables rollbacks, full historical audit trails, and reproducible machine learning experiments.
- [Upserts](/delta-update/#upsert-into-a-table-using-merge) and [deletes](/delta-update/#delete-from-a-table): Supports merge, update and delete operations to enable complex use cases like change-data-capture, slowly-changing-dimension (SCD) operations, streaming upserts, and so on.
- Vibrant connector ecosystem: Delta Lake has connectors read and write Delta tables from various data processing engines like Apache Spark, Apache Flink, Apache Hive, Apache Trino, AWS Athena, and more.

To get started follow the [quickstart guide](/apache-spark-connector/quick) to learn how to use Delta Lake with Apache Spark.
