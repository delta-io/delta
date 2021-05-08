/*
 * Copyright (2020) The Delta Lake Project Authors.
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
trait DeltaSQLConfBase {
  def buildConf(key: String): ConfigBuilder = SQLConf.buildConf(s"spark.databricks.delta.$key")
  def buildStaticConf(key: String): ConfigBuilder =
    SQLConf.buildStaticConf(s"spark.databricks.delta.$key")

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

  val DELTA_COMMIT_LOCK_ENABLED =
    buildConf("commitLock.enabled")
      .internal()
      .doc("Whether to lock a Delta table when doing a commit.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_USER_METADATA =
    buildConf("commitInfo.userMetadata")
      .doc("Arbitrary user-defined metadata to include in CommitInfo. Requires commitInfo.enabled.")
      .stringConf
      .createOptional

  val DELTA_CONVERT_USE_METADATA_LOG =
    buildConf("convert.useMetadataLog")
      .doc(
        """ When converting to a Parquet table that was created by Structured Streaming, whether
        |  to use the transaction log under `_spark_metadata` as the source of truth for files
        | contained in the table.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_SNAPSHOT_PARTITIONS =
    buildConf("snapshotPartitions")
      .internal()
      .doc("Number of partitions to use when building a Delta Lake snapshot.")
      .intConf
      .checkValue(n => n > 0, "Delta snapshot partition number must be positive.")
      .createOptional

  val DELTA_PARTITION_COLUMN_CHECK_ENABLED =
    buildConf("partitionColumnValidity.enabled")
      .internal()
      .doc("Whether to check whether the partition column names have valid names, just like " +
        "the data columns.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED =
    buildConf("stateReconstructionValidation.enabled")
      .internal()
      .doc("Whether to perform validation checks on the reconstructed state.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_COMMIT_VALIDATION_ENABLED =
    buildConf("commitValidation.enabled")
      .internal()
      .doc("Whether to perform validation checks before commit or not.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_SCHEMA_ON_READ_CHECK_ENABLED =
    buildConf("checkLatestSchemaOnRead")
      .doc("In Delta, we always try to give users the latest version of their data without " +
        "having to call REFRESH TABLE or redefine their DataFrames when used in the context of " +
        "streaming. There is a possibility that the schema of the latest version of the table " +
        "may be incompatible with the schema at the time of DataFrame creation. This flag " +
        "enables a check that ensures that users won't read corrupt data if the source schema " +
        "changes in an incompatible way.")
      .booleanConf
      .createWithDefault(true)

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

  val DELTA_CONVERT_METADATA_CHECK_ENABLED =
    buildConf("convert.metadataCheck.enabled")
      .doc(
        """
          |If enabled, during convert to delta, if there is a difference between the catalog table's
          |properties and the Delta table's configuration, we should error. If disabled, merge
          |the two configurations with the same semantics as update and merge.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

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

  val DELTA_MAX_RETRY_COMMIT_ATTEMPTS =
    buildConf("maxCommitAttempts")
      .internal()
      .doc("The maximum number of commit attempts we will try for a single commit before failing")
      .intConf
      .checkValue(_ >= 0, "maxCommitAttempts has to be positive")
      .createWithDefault(10000000)

  val DELTA_PROTOCOL_DEFAULT_WRITER_VERSION =
    buildConf("properties.defaults.minWriterVersion")
      .doc("The default writer protocol version to create new tables with, unless a feature " +
        "that requires a higher version for correctness is enabled.")
      .intConf
      .checkValues(Set(1, 2, 3, 4))
      .createWithDefault(2)

  val DELTA_PROTOCOL_DEFAULT_READER_VERSION =
    buildConf("properties.defaults.minReaderVersion")
      .doc("The default reader protocol version to create new tables with, unless a feature " +
        "that requires a higher version for correctness is enabled.")
      .intConf
      .checkValues(Set(1))
      .createWithDefault(1)

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

  val DELTA_HISTORY_METRICS_ENABLED =
    buildConf("history.metricsEnabled")
      .doc("Enables Metrics reporting in Describe History. CommitInfo will now record the " +
        "Operation Metrics.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_VACUUM_RETENTION_CHECK_ENABLED =
    buildConf("retentionDurationCheck.enabled")
      .doc("Adds a check preventing users from running vacuum with a very short retention " +
        "period, which may end up corrupting the Delta Log.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_VACUUM_PARALLEL_DELETE_ENABLED =
    buildConf("vacuum.parallelDelete.enabled")
      .doc("Enables parallelizing the deletion of files during a vacuum command. Enabling " +
        "may result hitting rate limits on some storage backends. When enabled, parallelization " +
        "is controlled by the default number of shuffle partitions.")
      .booleanConf
      .createWithDefault(false)

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

  val MERGE_INSERT_ONLY_ENABLED =
    buildConf("merge.optimizeInsertOnlyMerge.enabled")
      .internal()
      .doc(
        """
          |If enabled, merge without any matched clause (i.e., insert-only merge) will be optimized
          |by avoiding rewriting old files and just inserting new files.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  val MERGE_REPARTITION_BEFORE_WRITE =
    buildConf("merge.repartitionBeforeWrite.enabled")
      .internal()
      .doc(
        """
          |When enabled, merge will repartition the output by the table's partition columns before
          |writing the files.
        """.stripMargin)
      .booleanConf
      .createWithDefault(false)

  val MERGE_MATCHED_ONLY_ENABLED =
    buildConf("merge.optimizeMatchedOnlyMerge.enabled")
      .internal()
      .doc(
        """If enabled, merge without 'when not matched' clause will be optimized to use a
          |right outer join instead of a full outer join.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  val MERGE_SKIP_OSS_RESOLUTION_WITH_STAR =
    buildConf("merge.skipOssResolutionWithStar")
      .internal()
      .doc(
        """
          |If enabled, then any MERGE operation having UPDATE * / INSERT * will skip Apache
          |Spark's resolution logic and use Delta's specific resolution logic. This is to avoid
          |bug with star and temp views. See SC-72276 for details.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_LAST_COMMIT_VERSION_IN_SESSION =
    buildConf("lastCommitVersionInSession")
      .doc("The version of the last commit made in the SparkSession for any table.")
      .longConf
      .checkValue(_ >= 0, "the version must be >= 0")
      .createOptional

  val ALLOW_UNENFORCED_NOT_NULL_CONSTRAINTS =
    buildConf("constraints.allowUnenforcedNotNull.enabled")
      .internal()
      .doc("If enabled, NOT NULL constraints within array and map types will be permitted in " +
        "Delta table creation, even though Delta can't enforce them.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_CHECKPOINT_V2_ENABLED =
    buildConf("checkpointV2.enabled")
      .internal()
      .doc("Write checkpoints where the partition values are parsed according to the data type.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_WRITE_CHECKSUM_ENABLED =
    buildConf("writeChecksumFile.enabled")
      .doc("Whether the checksum file can be written.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME =
    buildConf("resolveMergeUpdateStructsByName.enabled")
      .internal()
      .doc("Whether to resolve structs by name in UPDATE operations of UPDATE and MERGE INTO " +
        "commands. If disabled, Delta will revert to the legacy behavior of resolving by position.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_TIME_TRAVEL_STRICT_TIMESTAMP_PARSING =
    buildConf("timeTravel.parsing.strict")
      .internal()
      .doc("Whether to require time travel timestamps to parse to a valid timestamp. If " +
        "disabled, Delta will revert to the legacy behavior of treating invalid timestamps as " +
        "equivalent to unix time 0 (1970-01-01 00:00:00).")
      .booleanConf
      .createWithDefault(true)

  val DELTA_STRICT_CHECK_DELTA_TABLE =
    buildConf("isDeltaTable.strictCheck")
      .internal()
      .doc("""
           | When enabled, io.delta.tables.DeltaTable.isDeltaTable
           | should return false when the _delta_log directory doesn't
           | contain any transaction logs.
           |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_LEGACY_STORE_WRITER_OPTIONS_AS_PROPS =
    buildConf("legacy.storeOptionsAsProperties")
      .internal()
      .doc("""
             |Delta was unintentionally storing options provided by the DataFrameWriter in the
             |saveAsTable method as table properties in the transaction log. This was unsupported
             |behavior (it was a bug), and it has security implications (accidental storage of
             |credentials). This flag prevents the storage of arbitrary options as table properties.
             |Set this flag to true to continue setting non-delta prefixed table properties through
             |table options.
             |""".stripMargin)
      .booleanConf
      .createWithDefault(false)
}

object DeltaSQLConf extends DeltaSQLConfBase
