// Databricks notebook source
// MAGIC %md
// MAGIC <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>
// MAGIC 
// MAGIC An open-source storage format that brings ACID transactions to Apache Spark™ and big data workloads.
// MAGIC 
// MAGIC * **Open format**: Stored as Parquet format in blob storage.
// MAGIC * **ACID Transactions**: Ensures data integrity and read consistency with complex, concurrent data pipelines.
// MAGIC * **Schema Enforcement and Evolution**: Ensures data cleanliness by blocking writes with unexpected.
// MAGIC * **Audit History**: History of all the operations that happened in the table.
// MAGIC * **Time Travel**: Query previous versions of the table by time or version number.
// MAGIC * **Deletes and upserts**: Supports deleting and upserting into tables with programmatic APIs.
// MAGIC * **Scalable Metadata management**: Able to handle millions of files are scaling the metadata operations with Spark.
// MAGIC * **Unified Batch and Streaming Source and Sink**: A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box. 

// COMMAND ----------

// MAGIC %md
// MAGIC <img src="https://i.imgur.com/pZt06Cw.png" width=400/> 
// MAGIC # Delta Lake Scala Tutorial 
// MAGIC 
// MAGIC 
// MAGIC ## Steps to run this notebook
// MAGIC 
// MAGIC You can run this notebook in a Databricks environment. Specifically, this notebook has been designed to run in [Databricks Community Edition](http://community.cloud.databricks.com/) as well.
// MAGIC To run this notebook, you have to [create a cluster](https://docs.databricks.com/clusters/create.html) with version **Databricks Runtime 6.1 or later** and [attach this notebook](https://docs.databricks.com/notebooks/notebooks-manage.html#attach-a-notebook-to-a-cluster) to that cluster.<br/>&nbsp;
// MAGIC 
// MAGIC ### Source Data for this notebook
// MAGIC 
// MAGIC The data used is a modified version of the public data from [Lending Club](https://www.kaggle.com/wendykan/lending-club-loan-data). It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).

// COMMAND ----------

// MAGIC %md ## 1. Explore data as a Parquet table

// COMMAND ----------

// MAGIC %md #####Download the sampled Lending Club data

// COMMAND ----------

// MAGIC %sh mkdir -p /dbfs/tmp/sais_eu_19_demo/loans/ && wget -O /dbfs/tmp/sais_eu_19_demo/loans/SAISEU19-loan-risks.snappy.parquet  https://pages.databricks.com/rs/094-YMS-629/images/SAISEU19-loan-risks.snappy.parquet && ls -al  /dbfs/tmp/sais_eu_19_demo/loans/ 

// COMMAND ----------

// MAGIC %md **Setup and configuration**

// COMMAND ----------

import java.io._

// Configurations necessary for running of Databricks Community Edition
spark.sql("set spark.sql.shuffle.partitions = 1")
spark.sql("set spark.databricks.delta.snapshotPartitions = 1")

val demo_path = "/sais_eu_19_demo/"
val parquetPath = "/sais_eu_19_demo/loans_parquet"

if (new File("/dbfs" + demo_path).exists()) {
  println("Deleting path " + demo_path)
  dbutils.fs.rm(parquetPath, true)
}

// COMMAND ----------

// MAGIC %md #####Create the parquet table "loans_parquet"

// COMMAND ----------

import java.io.File

val parquetPath = "/sais_eu_19_demo/loans_parquet"

// Configurations necessary for running of Databricks Community Edition
spark.sql("set spark.sql.shuffle.partitions = 1")
spark.sql("set spark.databricks.delta.snapshotPartitions = 1")

// Delete a new parquet table with the parquet file
if (new File("/dbfs" + parquetPath).exists()) {
  println("Deleting path " + parquetPath)
  dbutils.fs.rm(parquetPath, true)
}
  
// Create a new parquet table with the parquet file
spark.read.format("parquet").load("/tmp/sais_eu_19_demo/loans")
  .write.format("parquet").save(parquetPath)
println("Created a Parquet table at " + parquetPath)

// Create a view on the table called loans_parquet
spark.read.format("parquet").load(parquetPath).createOrReplaceTempView("loans_parquet")
println("Defined view 'loans_parquet'")


// COMMAND ----------

// MAGIC %md #####Let's explore this parquet table.
// MAGIC 
// MAGIC *Schema of the table*
// MAGIC - load_id - unique id for each loan
// MAGIC - funded_amnt - principal amount of the loan funded to the loanee
// MAGIC - paid_amnt - amount from the principle that has been paid back (ignoring interests)
// MAGIC - addr_state - state where this loan was funded

// COMMAND ----------

display(spark.sql("select * from loans_parquet"))

// COMMAND ----------

// MAGIC %md **How many records does it have?**

// COMMAND ----------

display(spark.sql("select count(*) from loans_parquet"))

// COMMAND ----------

dbutils.notebook.exit("stop") // Stop the notebook before the streaming cell, in case of a "run all" 

// COMMAND ----------

// MAGIC %md **Let's start appending some new data to it using Structured Streaming.**
// MAGIC 
// MAGIC We will generate a stream of data from with randomly generated loan ids and amounts. 
// MAGIC In addition, we are going to define a few more useful utility functions.

// COMMAND ----------

import java.util.Random
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._


val random = new Random()

def randomCheckpointDir: String = 
  s"/sais_eu_19_demo/chkpt/${random.nextInt(10000)}"

// User-defined function to generate random state
val states = Seq("CA", "TX", "NY", "IA")
val randomState = udf(() => states(random.nextInt(states.length)))

// Function to start a streaming query with a stream of randomly generated load data and append to the parquet table
def generateAndAppendDataStream(tableFormat: String, tablePath: String): StreamingQuery = {
  
  val streamData = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    .withColumn("loan_id", col("value") + 10000)
    .withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer"))
    .withColumn("paid_amnt", col("funded_amnt") - (rand() * 2000))
    .withColumn("addr_state", randomState())

  val query = streamData.writeStream
    .format(tableFormat)
    .option("checkpointLocation", randomCheckpointDir)
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start(tablePath)

  query
}

// Function to stop all streaming queries 
def stopAllStreams(): Unit = {
  // Stop all the streams
  println("Stopping all streams")
  spark.streams.active.foreach(_.stop())
  println("Stopped all streams")
  println("Deleting checkpoints")  
  dbutils.fs.rm("/sais_eu_19_demo/chkpt/", true)
  println("Deleted checkpoints")
}

// COMMAND ----------

// MAGIC %md **Let's start a new stream to append data to the Parquet table**

// COMMAND ----------

val streamQuery = generateAndAppendDataStream(
    tableFormat = "parquet", 
    tablePath = parquetPath)


// COMMAND ----------

// MAGIC %md **Let's see if the data is being added to the table or not**.

// COMMAND ----------

spark.read.format("parquet").load(parquetPath).count()

// COMMAND ----------

// MAGIC %md **Where did our existing 14705 rows go? Let's see the data once again**

// COMMAND ----------

display(spark.read.format("parquet").load(parquetPath)) // wrong schema!

// COMMAND ----------

// MAGIC %md **Where did the two new columns `timestamp` and `value` come from? What happened here!**
// MAGIC 
// MAGIC What really happened is that when the streaming query started adding new data to the Parquet table, it did not properly account for the existing data in the table. Furthermore, the new data files that written out accidentally had two extra columns in the schema. Hence, when reading the table, the 2 different schema from different files were merged together, thus unexpectedly modifying the schema of the table.
// MAGIC 
// MAGIC 
// MAGIC Before we move on, **if you are running on Databricks Community Edition, definitely stop the streaming queries.** 
// MAGIC 
// MAGIC You free account in Databricks Community Edition has quota limits on the number of files and we do not want to hit that quote limit by running the streaming queries for too long.

// COMMAND ----------

stopAllStreams()

// COMMAND ----------

// MAGIC %md ### Problems with Parquet format
// MAGIC 
// MAGIC Parquet is only a data layout format within a single file, does not provide any guarantees across an entire table of many parquet files.
// MAGIC 
// MAGIC #### 1. No schema enforcement 
// MAGIC Schema is not enforced when writing leading to dirty and often corrupted data.
// MAGIC 
// MAGIC #### 2. No interoperatbility between batch and streaming workloads
// MAGIC Apache Spark's Parquet streaming sink does not maintain enough metadata such that streaming workloads can seamlessly interact with batch workloads.

// COMMAND ----------

// MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 2. Batch + stream processing and schema enforcement with Delta Lake
// MAGIC Let's understand Delta Lake solves these particular problems (among many others). We will start by creating a Delta table from the original data.

// COMMAND ----------

// Configure Delta Lake Silver Path
val deltaPath = "/sais_eu_19_demo/loans_delta"

// Configurations necessary for running of Databricks Community Edition
spark.sql("set spark.sql.shuffle.partitions = 1")
spark.sql("set spark.databricks.delta.snapshotPartitions = 1")

// Remove folder if it exists
println("Deleting directory " + deltaPath)
dbutils.fs.rm(deltaPath, recurse = true)

// Create the Delta table with the same loans data
spark.read.format("parquet").load("/tmp/sais_eu_19_demo/loans")
  .write.format("delta").save(deltaPath)
println("Created a Delta table at " + deltaPath)

spark.read.format("delta").load(deltaPath).createOrReplaceTempView("loans_delta")
println("Defined view 'loans_delta'")


// COMMAND ----------

// MAGIC %md **Let's see the data once again.**

// COMMAND ----------

display(spark.sql("select count(*) from loans_delta"))

// COMMAND ----------

display(spark.sql("select * from loans_delta"))

// COMMAND ----------

// MAGIC %md **Let's run a streaming count(*) on the table so that the count updates automatically**

// COMMAND ----------

spark.readStream.format("delta").load(deltaPath).createOrReplaceTempView("loans_delta_stream")
display(spark.sql("select count(*) from loans_delta_stream"))


// COMMAND ----------

// MAGIC %md **Now let's try writing the streaming appends once again**

// COMMAND ----------

val streamQuery2 = generateAndAppendDataStream(tableFormat = "delta", tablePath = deltaPath)

// COMMAND ----------

// MAGIC %md
// MAGIC The writes were blocked because the schema of the new data did not match the schema of table (see the exception details). See more information about how it works [here](https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html).
// MAGIC 
// MAGIC **Now, let's fix the streaming query by selecting the columns we want to write.**

// COMMAND ----------

// Generate a stream of randomly generated load data and append to the parquet table
def generateAndAppendDataStreamFixed(tableFormat: String, tablePath: String): StreamingQuery = {
  
  val streamData = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    .withColumn("loan_id", col("value") + 10000)
    .withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer"))
    .withColumn("paid_amnt", col("funded_amnt") - (rand() * 2000))
    .withColumn("addr_state", randomState())
    .select("loan_id", "funded_amnt", "paid_amnt", "addr_state")   // *********** FIXED THE SCHEMA OF THE GENERATED DATA *************

  val query = streamData.writeStream
    .format(tableFormat)
    .option("checkpointLocation", randomCheckpointDir)
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start(tablePath)

  query
}

// COMMAND ----------

// MAGIC %md **Now we can successfully write to the table. Note the count in the above streaming query increasing as we write to this table.**

// COMMAND ----------

val streamQuery2 = generateAndAppendDataStreamFixed(tableFormat = "delta", tablePath = deltaPath)

// COMMAND ----------

// MAGIC %md **Scroll back up to see the numbers change in the `readStream` as more data is being appended by the `writeStream`.** 
// MAGIC 
// MAGIC **In fact, we can run multiple streams.**

// COMMAND ----------

val streamQuery3 = generateAndAppendDataStreamFixed(tableFormat = "delta", tablePath = deltaPath)

// COMMAND ----------

// MAGIC %md Just for sanity check, let's query as a batch 

// COMMAND ----------

display(spark.sql("select count(*) from loans_delta"))

// COMMAND ----------

// MAGIC %md **Again, remember to stop all the streaming queries.**

// COMMAND ----------

stopAllStreams()

// COMMAND ----------

// MAGIC %md ###  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Schema Evolution
// MAGIC 
// MAGIC Let's evolve the schema of the table
// MAGIC We will run a batch query that will
// MAGIC - Append some new loans
// MAGIC - Add a boolean column 'closed' that signifies whether the loan has been closed and paid off or not.
// MAGIC 
// MAGIC We are going to set the option `mergeSchema` to `true` to force the evolution of the Delta table's schema. 

// COMMAND ----------

val loanUpdates = Seq(
  (1111111L, 1000, 1000.0, "TX", false), 
  (2222222L, 2000, 0.0, "CA", true))
  .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")
  
loanUpdates.write.format("delta")
  .mode("append")
  .option("mergeSchema", "true")
  .save(deltaPath)


// COMMAND ----------

// MAGIC %md **Let's query the table once again to see the schema.**

// COMMAND ----------

display(spark.read.format("delta").load(deltaPath))

// COMMAND ----------

// MAGIC %md ### Advantages of Delta Lake over Parquet
// MAGIC 
// MAGIC #### 1. Full support for mixed batch and streaming workloads
// MAGIC Any number of workloads can read and write data into Delta Lake tables.
// MAGIC 
// MAGIC #### 2. Schema enforcement and schema evolution
// MAGIC Delta Lake ensures pristine data quality.
// MAGIC 
// MAGIC #### 3. ACID Transactions
// MAGIC Delta Lake guarantees correctness and data integrity under concurrent workloads.

// COMMAND ----------

// MAGIC %md ## ![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 3. Delete from Delta Lake table
// MAGIC 
// MAGIC You can remove data that matches a predicate from a Delta Lake table. For more information, check out the [docs](https://docs.delta.io/latest/delta-update.html#delete-from-a-table).

// COMMAND ----------

// MAGIC %md **Let's see the number of fully paid loans.**

// COMMAND ----------

display(spark.sql("SELECT COUNT(*) FROM loans_delta WHERE funded_amnt = paid_amnt"))

// COMMAND ----------

// MAGIC %md **To reduce the size of the table from growing infinitely, we may want to delete all the loans that have been fully paid off**.

// COMMAND ----------

import io.delta.tables.DeltaTable

val deltaPath = "/sais_eu_19_demo/loans_delta"

val deltaTable = DeltaTable.forPath(spark, deltaPath)
deltaTable.delete("funded_amnt = paid_amnt")

// COMMAND ----------

// MAGIC %md **Let's check the number of fully paid loans once again.**

// COMMAND ----------

display(spark.sql("SELECT COUNT(*) FROM loans_delta WHERE funded_amnt = paid_amnt"))

// COMMAND ----------

// MAGIC %md **Note**: Because we were able to easily `DELETE` the data, the above value should be `0`.

// COMMAND ----------

// MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 4. Audit Delta Lake Table History
// MAGIC All changes to the Delta table are recorded as commits in the table's transaction log. As you write into a Delta table or directory, every operation is automatically versioned. You can use the HISTORY command to view the table's history. For more information, check out the [docs](https://docs.delta.io/latest/delta-utility.html#history).

// COMMAND ----------

val deltaPath = "/sais_eu_19_demo/loans_delta"

val deltaTable = DeltaTable.forPath(spark, deltaPath)
display(deltaTable.history())

// COMMAND ----------

// MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 5. Travel back in time
// MAGIC Delta Lake’s time travel feature allows you to access previous versions of the table. Here are some possible uses of this feature:
// MAGIC 
// MAGIC * Auditing Data Changes
// MAGIC * Reproducing experiments & reports
// MAGIC * Rollbacks
// MAGIC 
// MAGIC You can query by using either a timestamp or a version number using Python, Scala, and/or SQL syntax. For this examples we will query a specific version using the Python syntax.  
// MAGIC 
// MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html) and the [docs](https://docs.delta.io/latest/delta-batch.html#deltatimetravel).
// MAGIC 
// MAGIC **Let's query the table's state before we deleted the data, which still contains the fully paid loans.**

// COMMAND ----------

val previousVersion = deltaTable.history(1).select("version").collect().head.getLong(0) - 1

spark.read.format("delta")
  .option("versionAsOf", previousVersion)
  .load(deltaPath)
  .createOrReplaceTempView("loans_delta_pre_delete")

display(spark.sql("SELECT COUNT(*) FROM loans_delta_pre_delete WHERE funded_amnt = paid_amnt"))

// COMMAND ----------

// MAGIC %md **We see the same number of fully paid loans that we had seen before delete.**

// COMMAND ----------

// MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 6.  Vacuum old version of Delta Lake tables
// MAGIC 
// MAGIC While it's nice to be able to time travel to any previous version, sometimes you want actually delete the data from storage completely for reducing storage costs or for compliance reasons (example, GDPR).
// MAGIC The Vacuum operation deletes data files that have been removed from the table for a certain amount of time. For more information, check out the [docs](https://docs.delta.io/latest/delta-utility.html#vacuum).

// COMMAND ----------

// MAGIC %md By default, `vacuum()` retains all the data needed for the last 7 days. For this example, since this table does not have 7 days worth of history, we will retain 0 hours, which means to only keep the latest state of the table. 

// COMMAND ----------

spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
deltaTable.vacuum(retentionHours = 0)

// COMMAND ----------

// MAGIC %md **Same query as before, but it now fails**

// COMMAND ----------

spark.read.format("delta").option("versionAsOf", previousVersion).load(deltaPath).createOrReplaceTempView("loans_delta_pre_delete")
display(spark.sql("SELECT COUNT(*) FROM loans_delta_pre_delete WHERE funded_amnt = paid_amnt"))

// COMMAND ----------

// MAGIC %md ##  ![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 7. Upsert into Delta Lake table using Merge
// MAGIC You can upsert data from an Apache Spark DataFrame into a Delta Lake table using the merge operation. This operation is similar to the SQL MERGE command but has additional support for deletes and extra conditions in updates, inserts, and deletes. For more information checkout the [docs](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge).

// COMMAND ----------

// MAGIC %md #### Upsert with Parquet: 7-step process
// MAGIC 
// MAGIC With a legacy data pipeline, to insert or update a table, you must:
// MAGIC 1. Identify the new rows to be inserted
// MAGIC 2. Identify the rows that will be replaced (i.e. updated)
// MAGIC 3. Identify all of the rows that are not impacted by the insert or update
// MAGIC 4. Create a new temp based on all three insert statements
// MAGIC 5. Delete the original table (and all of those associated files)
// MAGIC 6. "Rename" the temp table back to the original table name
// MAGIC 7. Drop the temp table
// MAGIC 
// MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)
// MAGIC 
// MAGIC 
// MAGIC #### Upsert using with Delta Lake
// MAGIC 
// MAGIC 1-step process: 
// MAGIC 1. [Use `Merge` operation](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge)

// COMMAND ----------

spark.sql("set spark.sql.shuffle.partitions = 1")
spark.sql("set spark.databricks.delta.snapshotPartitions = 1")

// Configure Delta Lake Silver Path
val deltaSmallPath = "/sais_eu_19_demo/loans_delta_small"

// Remove folder if it exists
println("Deleting directory " + deltaSmallPath)
dbutils.fs.rm(deltaSmallPath, recurse=true)

// Create the Delta table with the same loans data
spark.read.format("parquet").load("/tmp/sais_eu_19_demo/loans")
  .where("loan_id < 3")
  .write.format("delta").save(deltaSmallPath)
println("Created a Delta table at " + deltaSmallPath)

spark.read.format("delta").load(deltaSmallPath).createOrReplaceTempView("loans_delta_small")
println("Defined view 'loans_delta_small'")

// COMMAND ----------

// MAGIC %md **Let's focus only on a part of the loans_delta table**

// COMMAND ----------

display(spark.sql("select * from loans_delta_small order by loan_id"))

// COMMAND ----------

// MAGIC %md **Now, let's say we got some new loan information**
// MAGIC 1. Existing loan_id = 2 has been fully repaid. The corresponding row needs to be updated.
// MAGIC 2. New loan_id = 3 has been funded in CA. This is need to be inserted as a new row.

// COMMAND ----------

val loanUpdates = Seq(
  (2, 1000, 1000.0, "TX"), // existing loan's paid_amnt updated, loan paid in full
  (3, 2000, 0.0, "CA"))    // new loan's details
  .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state")

display(loanUpdates)

// COMMAND ----------

// MAGIC %md **Merge can upsert this in a single atomic operation.**
// MAGIC 
// MAGIC SQL `MERGE` command can do both `UPDATE` and `INSERT`.
// MAGIC 
// MAGIC ```
// MAGIC 
// MAGIC MERGE INTO target t
// MAGIC USING source s
// MAGIC WHEN MATCHED THEN UPDATE SET ...
// MAGIC WHEN NOT MATCHED THEN INSERT ....
// MAGIC ```
// MAGIC 
// MAGIC Since Apache Spark's SQL parser does not have support for parsing MERGE SQL command, we have provided programmatic APIs in Python to perform the same operation with the same semantics as the SQL command.

// COMMAND ----------

import io.delta.tables.DeltaTable
val deltaTable = DeltaTable.forPath(spark, deltaSmallPath)

deltaTable.alias("t").merge(
  loanUpdates.alias("s"), 
  "s.loan_id = t.loan_id")
  .whenMatched.updateAll()
  .whenNotMatched.insertAll()
  .execute()

// COMMAND ----------

display(spark.sql("select * from loans_delta_small order by loan_id"))

// COMMAND ----------

// MAGIC %md **Note the changes in the table**
// MAGIC - Existing loan_id = 2 should have been updated with paid_amnt set to 1000. 
// MAGIC - New loan_id = 3 have been inserted.

// COMMAND ----------

// MAGIC %md ##  ![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 8. Advanced uses of Merge
// MAGIC We support [very cool extended syntax for Merge](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge) that allows you to use merge for a lot of advaned use cases. For example, 
// MAGIC - [Continuously apply upserts into Delta Lake tables from a Structured Streaming queries](https://docs.delta.io/latest/delta-update.html#upsert-from-streaming-queries-using-foreachbatch) (demonstrated below)
// MAGIC - [Data deduplication when writing into Delta Lake table](https://docs.delta.io/latest/delta-update.html#data-deduplication-when-writing-into-delta-tables) (demonstrated below)
// MAGIC - [Apply slowly changing data (SCD) Type 2 operations into Delta Lake tables](https://docs.delta.io/latest/delta-update.html#slowly-changing-data-scd-type-2-operation-into-delta-tables)
// MAGIC - [Write change data to replicate other table changes in Delta Lake table](https://docs.delta.io/latest/delta-update.html#write-change-data-into-a-delta-table)

// COMMAND ----------

// MAGIC %md ### 8.1 Streaming upserts into a Delta Lake table using merge and foreachBatch
// MAGIC 
// MAGIC You can continuously upsert from a streaming query output using merge inside `foreachBatch` operation of Structured Streaming.
// MAGIC 
// MAGIC Let's say, we want to maintain a count of the loans per state in a table, and new loans arrive, we want to update the counts. 
// MAGIC 
// MAGIC To do this, we will first initialize the table and a few associated UDFs and configurations.

// COMMAND ----------

// Generate a stream of randomly generated load data and append to the parquet table
import java.util.Random
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Configuration 
spark.sql("set spark.sql.shuffle.partitions = 1")
spark.sql("set spark.databricks.delta.snapshotPartitions = 1")

val loanCountsByStatesPath = "/sais_eu_19_demo/loans_by_states"

// Initialize the table
Seq(("CA", 0)).toDF("addr_state" , "count")
  .write.format("delta").mode("overwrite").save(loanCountsByStatesPath)
  
// User-defined function to generate random state
val states = Seq("CA", "TX", "NY", "IA")

val random = new Random()
val randomState = udf(() => states(random.nextInt(states.length)))

// COMMAND ----------

// MAGIC %md Define the function to be called on the output of each micro-batch. This function will use merge to upsert into the Delta table.

// COMMAND ----------

import org.apache.spark.sql.DataFrame

val loanCountsByStatesTable = DeltaTable.forPath(spark, loanCountsByStatesPath)

// Function to upsert per-state counts generated in each microbatch of a streaming query
// - updated_counts_df = the updated counts generated from a microbatch
def upsertStateCountsIntoDeltaTable(updatedCountsDf: DataFrame, batchId: Long): Unit = {
  loanCountsByStatesTable.alias("t").merge(
      updatedCountsDf.alias("s"), 
      "s.addr_state = t.addr_state")
    .whenMatched.updateAll()
    .whenNotMatched.insertAll()
    .execute()
}

// COMMAND ----------

// MAGIC %md Define and run the streaming query using this function with `foreachBatch`.

// COMMAND ----------

// loan_ids that have been complete paid off, random generated
val loansUpdateStreamData = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
  .withColumn("loan_id", rand() * 100)
  .withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer"))
  .withColumn("paid_amnt", col("funded_amnt") - (rand() * 2000))
  .withColumn("addr_state", randomState())
  .createOrReplaceTempView("generated_loads")

// use foreachBatch to define what to do with each output micro-batch DataFrame
val query = spark.sql("select addr_state, count(*) as count from generated_loads group by addr_state")
  .writeStream
  .format("delta")
  .foreachBatch(upsertStateCountsIntoDeltaTable _)
  .option("checkpointLocation", randomCheckpointDir)
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .outputMode("update")
  .start(loanCountsByStatesPath)

// COMMAND ----------

// MAGIC %md Let's query the state to see the counts. If you run the following cell repeatedly, you will see that the counts will keep growing.

// COMMAND ----------

// Run this cell repeatedly to see updated data
display(spark.read.format("delta").load(loanCountsByStatesPath).orderBy("addr_state"))

// COMMAND ----------

stopAllStreams()

// COMMAND ----------

// MAGIC %md ###8.2 Deduplication using "insert-only" merge
// MAGIC A common ETL use case is to collect logs into Delta Lake table by appending them to a table. However, often the sources can generate duplicate log records and downstream deduplication steps are needed to take care of them. With merge, you can avoid inserting the duplicate records.

// COMMAND ----------

// MAGIC %md Prepare a new Delta Lake table

// COMMAND ----------

import io.delta.tables.DeltaTable
val deltaPath = "/sais_eu_19_demo/loans_delta"

// Remove folder if it exists
println("Deleting directory " + deltaPath)
dbutils.fs.rm(deltaPath, recurse = true)

// Define new loans table data
val data = Seq(
  (0, 1000, 1000.0, "TX"), 
  (1, 2000, 0.0, "CA"))
  .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state")

// Write a new Delta Lake table with the loans data
data.write.format("delta").save(deltaPath)

// Define DeltaTable object
val dt = DeltaTable.forPath(spark, deltaPath)

display(dt.toDF)

// COMMAND ----------

// MAGIC %md Prepare the new data to apply which has duplicates

// COMMAND ----------

// Define a DataFrame containing new data, some of which is already present in the table
val newData = Seq(  
  (1, 2000, 0.0, "CA"),     // duplicate, loan_id = 1 is already present in table and don't want to update
  (2, 5000, 1010.0, "NY"))  // new data, not present in table
  .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state")

display(newData)

// COMMAND ----------

// MAGIC %md Run "insert-only" merqe query (i.e., no update clause)

// COMMAND ----------

val dt = DeltaTable.forPath(spark, deltaPath)

dt.alias("t").merge(
    newData.alias("s"), 
    "s.loan_id = t.loan_id")
  .whenNotMatched.insertAll()
  .execute()

display(dt.toDF)

// COMMAND ----------

// MAGIC %md
// MAGIC <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>
// MAGIC <br/>
// MAGIC # 9. Tutorial Summary
// MAGIC 
// MAGIC #### Full support for batch and streaming workloads
// MAGIC * Delta Lake allows batch and streaming workloads to concurrently read and write to Delta Lake tables with full ACID transactional guarantees.
// MAGIC 
// MAGIC #### Schema enforcement and schema evolution
// MAGIC * Delta Lake provides the ability to specify your schema and enforce it. This helps ensure that the data types are correct and required columns are present, preventing bad data from causing data corruption.
// MAGIC 
// MAGIC #### Table History and Time Travel
// MAGIC * Delta Lake transaction log records details about every change made to data providing a full audit trail of the changes. 
// MAGIC * You can query previous snapshots of data enabling developers to access and revert to earlier versions of data for audits, rollbacks or to reproduce experiments.
// MAGIC 
// MAGIC #### Delete data and Vacuum old versions
// MAGIC * Delete data from tables using a predicate.
// MAGIC * Fully remove data from previous versions using Vaccum to save storage and satisfy compliance requirements.
// MAGIC 
// MAGIC #### Upsert data using Merge
// MAGIC * Upsert data into tables from batch and streaming workloads
// MAGIC * Use extended merge syntax for advanced usecases like data deduplication, change data capture, SCD type 2 operations, etc.

// COMMAND ----------

// MAGIC %md #Join the community!
// MAGIC 
// MAGIC 
// MAGIC * [Delta Lake on GitHub](https://github.com/delta-io/delta)
// MAGIC * [Delta Lake Slack Channel](https://delta-users.slack.com/) ([Registration Link](https://join.slack.com/t/delta-users/shared_invite/enQtNTY1NDg0ODcxOTI1LWJkZGU3ZmQ3MjkzNmY2ZDM0NjNlYjE4MWIzYjg2OWM1OTBmMWIxZTllMjg3ZmJkNjIwZmE1ZTZkMmQ0OTk5ZjA))
// MAGIC * [Public Mailing List](https://groups.google.com/forum/#!forum/delta-users)
