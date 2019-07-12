/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.sources

// scalastyle:off import.ordering.noEmptyLine
import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.sql.internal.SQLConf

/**
 * [[SQLConf]] entries for Delta features.
 */
object DeltaSQLConf {
  def buildConf(key: String): ConfigBuilder = SQLConf.buildConf(s"spark.databricks.delta.$key")


  val RESOLVE_TIME_TRAVEL_ON_IDENTIFIER =
    buildConf("timeTravel.resolveOnIdentifier.enabled")
      .internal()
      .doc("When true, we will try to resolve patterns as `@v123` in identifiers as time " +
        "travel nodes.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_COMMIT_INFO_ENABLED =
    buildConf("commitInfo.enabled")
      .doc("Whether to log commit information into the Delta log.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_SNAPSHOT_PARTITIONS =
    buildConf("snapshotPartitions")
      .internal()
      .doc("Number of partitions to use when building a Delta Lake snapshot.")
      .intConf
      .checkValue(n => n > 0, "Delta snapshot partition number must be positive.")
      .createWithDefault(50)

  val DELTA_COLLECT_STATS =
    buildConf("stats.collect")
      .internal()
      .doc("When true, statistics are collected while writing files into a Delta table.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_IMPORT_BATCH_SIZE_STATS_COLLECTION =
    buildConf("import.batchSize.statsCollection")
      .internal()
      .doc("The number of files per batch for stats collection during import.")
      .intConf
      .createWithDefault(50000)

  val DELTA_IMPORT_BATCH_SIZE_SCHEMA_INFERENCE =
    buildConf("import.batchSize.schemaInference")
      .internal()
      .doc("The number of files per batch for schema inference during import.")
      .intConf
      .createWithDefault(1000000)

  val DELTA_SAMPLE_ESTIMATOR_ENABLED =
    buildConf("sampling.enabled")
      .internal()
      .doc("Enable sample based estimation.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_STATS_SKIPPING =
    buildConf("stats.skipping")
      .internal()
      .doc("When true, statistics are used for skipping")
      .booleanConf
      .createWithDefault(true)

  val DELTA_LIMIT_PUSHDOWN_ENABLED =
    buildConf("stats.limitPushdown.enabled")
      .internal()
      .doc("If true, use the limit clause and file statistics to prune files before " +
        "they are collected to the driver. ")
      .booleanConf
      .createWithDefault(true)

  val DELTA_STATS_SKIPPING_LOCAL_CACHE_MAX_NUM_FILES =
    buildConf("stats.localCache.maxNumFiles")
      .internal()
      .doc("The maximum number of files for a table to be considered a 'delta small table'." +
        "Some metadata operations (such as using data skipping) are optimized for small tables " +
        "using driver local caching and local execution.")
      .intConf
      .createWithDefault(2000)

  val DELTA_SNAPSHOT_ISOLATION =
    buildConf("snapshotIsolation.enabled")
      .internal()
      .doc("Controls whether queries on Delta tables are guaranteed to have " +
        "snapshot isolation.")
      .booleanConf
      .createWithDefault(true)


  val DELTA_MAX_SNAPSHOT_LINEAGE_LENGTH =
    buildConf("maxSnapshotLineageLength")
      .internal()
      .doc("The max lineage length of a Snapshot before Delta forces to build a Snapshot from " +
        "scratch.")
      .intConf
      .checkValue(_ > 0, "maxSnapshotLineageLength must be positive.")
      .createWithDefault(50)

  val DELTA_HISTORY_PAR_SEARCH_THRESHOLD =
    buildConf("history.maxKeysPerList")
      .internal()
      .doc("How many commits to list when performing a parallel search. Currently set to 1000, " +
        "which is the maximum keys returned by S3 per list call. Azure can return 5000, " +
        "therefore we choose 1000.")
      .intConf
      .createWithDefault(1000)

  val DELTA_VACUUM_RETENTION_CHECK_ENABLED =
    buildConf("retentionDurationCheck.enabled")
      .doc("Adds a check preventing users from running vacuum with a very short retention " +
        "period, which may end up corrupting the Delta Log.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_CHECKPOINT_PART_SIZE =
    buildConf("checkpoint.partSize")
      .internal()
      .doc(
        """The limit at which we will start parallelizing the checkpoint. We will attempt to write
          |maximum of this many actions per checkpoint.
        """.stripMargin)
      .longConf
      .checkValue(_ > 0, "The checkpoint part size needs to be a positive integer.")
      .createWithDefault(5000000)

  val DELTA_SCHEMA_AUTO_MIGRATE =
    buildConf("schema.autoMerge.enabled")
      .doc("If true, enables schema merging on appends and on overwrites.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_STATE_CORRUPTION_IS_FATAL =
    buildConf("state.corruptionIsFatal")
      .internal()
      .doc(
        """If true, throws a fatal error when the recreated Delta State doesn't
          |match committed checksum file.
        """)
      .booleanConf
      .createWithDefault(true)

  val DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT =
    buildConf("stalenessLimit")
      .doc(
        """Setting a non-zero time limit will allow you to query the last loaded state of the Delta
          |table without blocking on a table update. You can use this configuration to reduce the
          |latency on queries when up-to-date results are not a requirement. Table updates will be
          |scheduled on a separate scheduler pool in a FIFO queue, and will share cluster resources
          |fairly with your query. If a table hasn't updated past this time limit, we will block
          |on a synchronous state update before running the query.
        """.stripMargin)
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(0L) // Don't let tables go stale

  val DELTA_ALTER_LOCATION_BYPASS_SCHEMA_CHECK =
    buildConf("alterLocation.bypassSchemaCheck")
      .doc("If true, Alter Table Set Location on Delta will go through even if the Delta table " +
        "in the new location has a different schema from the original Delta table.")
      .booleanConf
      .createWithDefault(false)

  val DUMMY_FILE_MANAGER_NUM_OF_FILES =
    buildConf("dummyFileManager.numOfFiles")
      .internal()
      .doc("How many dummy files to write in DummyFileManager")
      .intConf
      .checkValue(_ >= 0, "numOfFiles can not be negative.")
      .createWithDefault(3)

  val DUMMY_FILE_MANAGER_PREFIX =
    buildConf("dummyFileManager.prefix")
      .internal()
      .doc("The file prefix to use in DummyFileManager")
      .stringConf
      .createWithDefault(".s3-optimization-")

  val MERGE_MAX_INSERT_COUNT =
    buildConf("merge.maxInsertCount")
      .internal()
      .doc("Max row count of inserts in each MERGE execution.")
      .longConf
      .createWithDefault(10000L)

}
