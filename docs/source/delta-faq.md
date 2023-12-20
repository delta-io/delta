---
description: Learn the answers to frequently asked questions about <Delta>.
---

# Frequently asked questions (FAQ)


.. contents:: In this article:
  :local:
  :depth: 1

## What is <Delta>?

[Delta Lake](https://delta.io/) is an [open source storage layer](https://github.com/delta-io/delta) that brings reliability to [data lakes](https://databricks.com/discover/data-lakes/introduction). <Delta> provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. <Delta> runs on top of your existing data lake and is fully compatible with <AS> APIs.

## How is <Delta> related to <AS>?

<Delta> sits on top of <AS>. The format and the compute layer helps to simplify building big data pipelines and increase the overall efficiency of your pipelines.

## What format does <Delta> use to store data?

<Delta> uses versioned Parquet files to store your data in your cloud storage. Apart from the versions, <Delta> also stores a transaction log to keep track of all the commits made to the table or blob store directory to provide ACID transactions.

## How can I read and write data with <Delta>?

You can use your favorite <AS> APIs to read and write data with <Delta>. See [_](delta-batch.md#deltadataframereads) and [_](delta-batch.md#deltadataframewrites).

## Where does <Delta> store the data?

When writing data, you can specify the location in your cloud storage. <Delta> stores the data in that location in Parquet format.
  
## Can I copy my <Delta> table to another location?
 
Yes you can copy your <Delta> table to another location. Remember to copy files without changing the timestamps to ensure that the time travel with timestamps will be consistent.  

## Can I stream data directly into and from Delta tables?

Yes, you can use Structured Streaming to directly write data into Delta tables and read from Delta tables. See [Stream data into Delta tables](delta-streaming.md#stream-sink) and [Stream data from Delta tables](delta-streaming.md#stream-source).

## Does <Delta> support writes or reads using the Spark Streaming DStream API?

Delta does not support the DStream API. We recommend [_](delta-streaming.md).

## When I use <Delta>, will I be able to port my code to other Spark platforms easily?

Yes. When you use <Delta>, you are using open <AS> APIs so you can easily port your code to other Spark platforms. To port your code, replace `delta` format with `parquet` format.

## Does <Delta> support multi-table transactions?

<Delta> does not support multi-table transactions and foreign keys. <Delta> supports transactions at the _table_ level.

## How can I change the type of a column?

Changing a column's type or dropping a column requires rewriting the table. For an example, see [Change column type](delta-batch.md#change-column-type).



.. include:: /shared/replacements.md
