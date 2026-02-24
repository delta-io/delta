/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

/**
 * Utility trait providing common configuration building methods for Delta SQL configs.
 *
 * This trait contains only utility methods and constants, no actual config entries.
 * It is designed to be extended by multiple configuration objects without causing
 * duplicate config registration.
 */
trait DeltaSQLConfUtils {
  val SQL_CONF_PREFIX = "spark.databricks.delta"

  def buildConf(key: String): ConfigBuilder = SQLConf.buildConf(s"$SQL_CONF_PREFIX.$key")
  def buildStaticConf(key: String): ConfigBuilder =
    SQLConf.buildStaticConf(s"spark.databricks.delta.$key")
}

/**
 * [[SQLConf]] entries for Delta features.
 */
trait DeltaSQLConfBase extends DeltaSQLConfUtils {

  object DeltaBreakingChangeEnum {
    val OFF = "OFF"
    val LOG_ONLY = "LOG_ONLY"
    val ASSERT = "ASSERT"
    val validValues: Set[String] = Set(OFF, LOG_ONLY, ASSERT)
  }

  abstract class DeltaBreakingChangeEnum(configEntry: ConfigEntry[String])
    extends Enumeration {
    val OFF = Value("OFF")
    val LOG_ONLY = Value("LOG_ONLY")
    val ASSERT = Value("ASSERT")

    def fromConf(conf: SQLConf): Value =
      withName(conf.getConf(configEntry))

    def default: Value =
      withName(configEntry.defaultValueString)

    def confName: String = configEntry.key
  }

  val RESOLVE_TIME_TRAVEL_ON_IDENTIFIER =
    buildConf("timeTravel.resolveOnIdentifier.enabled")
      .internal()
      .doc("When true, we will try to resolve patterns as `@v123` in identifiers as time " +
        "travel nodes.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_COMMIT_LOCK_ENABLED =
    buildConf("commitLock.enabled")
      .internal()
      .doc("Whether to lock a Delta table when doing a commit.")
      .booleanConf
      .createOptional

  val DELTA_COLLECT_STATS =
    buildConf("stats.collect")
      .internal()
      .doc("When true, statistics are collected while writing files into a Delta table.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_DML_METRICS_FROM_METADATA =
    buildConf("dmlMetricsFromMetadata.enabled")
      .internal()
      .doc(
        """ When enabled, metadata only Delete, ReplaceWhere and Truncate operations will report row
        | level operation metrics by reading the file statistics for number of rows.
        | """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_COLLECT_STATS_USING_TABLE_SCHEMA =
    buildConf("stats.collect.using.tableSchema")
      .internal()
      .doc("When collecting stats while writing files into Delta table" +
        s" (${DELTA_COLLECT_STATS.key} needs to be true), whether to use the table schema (true)" +
        " or the DataFrame schema (false) as the stats collection schema.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_USER_METADATA =
    buildConf("commitInfo.userMetadata")
      .doc("Arbitrary user-defined metadata to include in CommitInfo.")
      .stringConf
      .createOptional

  val DELTA_FORCE_ALL_COMMIT_STATS =
    buildConf("commitStats.force")
      .internal()
      .doc(
        """When true, forces commit statistics to be collected for logging purposes.
        | Enabling this feature requires the Snapshot State to be computed, which is
        | potentially expensive.
        """.stripMargin)
      .booleanConf
      .createWithDefault(false)

  val DELTA_CONVERT_USE_METADATA_LOG =
    buildConf("convert.useMetadataLog")
      .doc(
        """ When converting to a Parquet table that was created by Structured Streaming, whether
        |  to use the transaction log under `_spark_metadata` as the source of truth for files
        | contained in the table.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_CONVERT_USE_CATALOG_PARTITIONS =
    buildConf("convert.useCatalogPartitions")
      .internal()
      .doc(
        """ When converting a catalog Parquet table, whether to use the partition information from
          | the Metastore catalog and only commit files under the directories of active partitions.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_CONVERT_USE_CATALOG_SCHEMA =
    buildConf("convert.useCatalogSchema")
      .doc(
        """ When converting to a catalog Parquet table, whether to use the catalog schema as the
          | source of truth.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_CONVERT_PARTITION_VALUES_IGNORE_CAST_FAILURE =
    buildConf("convert.partitionValues.ignoreCastFailure")
      .doc(
        """ When converting to Delta, ignore the failure when casting a partition value to
        | the specified data type, in which case the partition column will be filled with null.
        """.stripMargin)
      .booleanConf
      .createWithDefault(false)

  val DELTA_CONVERT_ICEBERG_USE_NATIVE_PARTITION_VALUES =
    buildConf("convert.iceberg.useNativePartitionValues")
      .doc(
        """ When enabled, obtain the partition values from Iceberg table's metadata, instead
          | of inferring from file paths.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_SNAPSHOT_PARTITIONS =
    buildConf("snapshotPartitions")
      .internal()
      .doc("Number of partitions to use when building a Delta Lake snapshot.")
      .intConf
      .checkValue(n => n > 0, "Delta snapshot partition number must be positive.")
      .createOptional

  val DELTA_SNAPSHOT_LOADING_MAX_RETRIES =
    buildConf("snapshotLoading.maxRetries")
      .internal()
      .doc("How many times to retry when failing to load a snapshot. Each retry will try to use " +
        "a different checkpoint in order to skip potential corrupt checkpoints.")
      .intConf
      .checkValue(n => n >= 0, "must not be negative.")
      .createWithDefault(2)

  val DELTA_SNAPSHOT_CACHE_STORAGE_LEVEL =
    buildConf("snapshotCache.storageLevel")
      .internal()
      .doc("StorageLevel to use for caching the DeltaLog Snapshot. In general, this should not " +
        "be used unless you are pretty sure that caching has a negative impact.")
      .stringConf
      .createWithDefault("MEMORY_AND_DISK_SER")

  val DELTA_SNAPSHOT_LOGGING_MAX_FILES_THRESHOLD =
    buildConf("snapshot.logging.maxFilesThreshold")
      .internal()
      .doc("Threshold for number of files in a snapshot. When exceeded, emits a warning with " +
        "remediation hints. Set to 0 to disable snapshot logging completely.")
      .longConf
      .checkValue(_ >= 0, "must be non-negative")
      .createWithDefault(500000L)

  val DELTA_PARTITION_COLUMN_CHECK_ENABLED =
    buildConf("partitionColumnValidity.enabled")
      .internal()
      .doc("Whether to check whether the partition column names have valid names, just like " +
        "the data columns.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_COMMIT_VALIDATION_ENABLED =
    buildConf("commitValidation.enabled")
      .internal()
      .doc("Whether to perform validation checks before commit or not.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_EMPTY_FILE_CHECK_THROW_ENABLED =
    buildConf("emptyFileCheck.throwEnabled")
      .internal()
      .doc("When true, throws IllegalStateException if a commit contains AddFile actions " +
        "referencing parquet files with size 0 bytes. " +
        "When false, only logs. Logging always occurs regardless of this setting.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_NULL_PARTITION_CHECK_THROW_ENABLED =
    buildConf("nullPartitionCheck.throwEnabled")
      .internal()
      .doc("When true, throws IllegalStateException if a commit contains AddFile actions with " +
        "null partition values for columns that have NOT NULL constraints. " +
        "When false, only logs. Logging always occurs regardless of this setting.")
      .booleanConf
      .createWithDefault(false)

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

  val DELTA_INCLUDE_TABLE_ID_IN_FILE_INDEX_COMPARISON =
    buildConf("includeTableIdInFileIndexComparison")
      .internal()
      .doc(
        """
          |Include the deltaLog.tableId field in equals and hashCode for TahoeLogFileIndex.
          |The field is unstable, so including it can lead semantic violations for equals and
          |hashCode.""".stripMargin)
      .booleanConf
      // TODO: Phase this out towards `false` eventually remove the flag altogether again.
      .createWithDefault(true)

  val DELTA_ALLOW_CREATE_EMPTY_SCHEMA_TABLE =
    buildConf("createEmptySchemaTable.enabled")
      .internal()
      .doc(
        s"""If enabled, creating a Delta table with an empty schema will be allowed through SQL API
           |`CREATE TABLE table () USING delta ...`, or Delta table APIs.
           |Creating a Delta table with empty schema table using dataframe operations or
           |`CREATE OR REPLACE` syntax are not supported.
           |The result Delta table can be updated using schema evolution operations such as
           |`df.save()` with `mergeSchema = true`.
           |Reading the empty schema table using DataframeReader or `SELECT` is not allowed.
           |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val AUTO_COMPACT_ALLOWED_VALUES = Seq(
    "false",
    "true"
  )

  val DELTA_AUTO_COMPACT_ENABLED =
    buildConf("autoCompact.enabled")
      .doc(s"""Whether to compact files after writes made into Delta tables from this session. This
        | conf can be set to "true" to enable Auto Compaction, OR "false" to disable Auto Compaction
        | on all writes across all delta tables in this session.
        | """.stripMargin)
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValue(AUTO_COMPACT_ALLOWED_VALUES.contains(_),
        """"spark.databricks.delta.autoCompact.enabled" must be one of: """ +
          s"""${AUTO_COMPACT_ALLOWED_VALUES.mkString("(", ",", ")")}""")
      .createOptional

  val DELTA_AUTO_COMPACT_RECORD_PARTITION_STATS_ENABLED =
    buildConf("autoCompact.recordPartitionStats.enabled")
      .internal()
      .doc(s"""When enabled, each committed write delta transaction records the number of qualified
              |files of each partition of the target table for Auto Compact in driver's
              |memory.""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_AUTO_COMPACT_EARLY_SKIP_PARTITION_TABLE_ENABLED =
    buildConf("autoCompact.earlySkipPartitionTable.enabled")
      .internal()
      .doc(s"""Auto Compaction will be skipped if there is no partition with
              |sufficient number of small files.""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_AUTO_COMPACT_MAX_TABLE_PARTITION_STATS =
    buildConf("autoCompact.maxTablePartitionStats")
      .internal()
      .doc(
        s"""The maximum number of Auto Compaction partition statistics of each table. This controls
           |the maximum number of partitions statistics each delta table can have. Increasing
           |this value reduces the hash conflict and makes partitions statistics more accurate with
           |the cost of more memory consumption.
           |""".stripMargin)
      .intConf
      .checkValue(_ > 0, "The value of maxTablePartitionStats should be positive.")
      .createWithDefault(16 * 1024)

  val DELTA_AUTO_COMPACT_PARTITION_STATS_SIZE =
    buildConf("autoCompact.partitionStatsSize")
      .internal()
      .doc(
        s"""The total number of partitions statistics entries can be kept in memory for all
           |tables in each driver. If this threshold is reached, the partitions statistics of
           |least recently accessed tables will be evicted out.""".stripMargin)
      .intConf
      .checkValue(_ > 0, "The value of partitionStatsSize should be positive.")
      .createWithDefault(64 * 1024)

  val DELTA_AUTO_COMPACT_MAX_FILE_SIZE =
    buildConf("autoCompact.maxFileSize")
      .internal()
      .doc(s"Target file size produced by auto compaction. The default value of this config" +
        " is 128 MB.")
      .longConf
      .checkValue(_ >= 0, "maxFileSize has to be positive")
      .createWithDefault(128 * 1024 * 1024)

  val DELTA_AUTO_COMPACT_MIN_NUM_FILES =
    buildConf("autoCompact.minNumFiles")
      .internal()
      .doc("Number of small files that need to be in a directory before it can be optimized.")
      .intConf
      .checkValue(_ >= 0, "minNumFiles has to be positive")
      .createWithDefault(50)

  val DELTA_AUTO_COMPACT_MIN_FILE_SIZE =
    buildConf("autoCompact.minFileSize")
      .internal()
      .doc("Files which are smaller than this threshold (in bytes) will be grouped together and " +
        "rewritten as larger files by the Auto Compaction. The default value of this config " +
        s"is set to half of the config ${DELTA_AUTO_COMPACT_MAX_FILE_SIZE.key}")
      .longConf
      .checkValue(_ >= 0, "minFileSize has to be positive")
      .createOptional

  val DELTA_AUTO_COMPACT_MODIFIED_PARTITIONS_ONLY_ENABLED =
    buildConf("autoCompact.modifiedPartitionsOnly.enabled")
      .internal()
      .doc(
        s"""When enabled, Auto Compaction only works on the modified partitions of the delta
           |transaction that triggers compaction.""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_AUTO_COMPACT_NON_BLIND_APPEND_ENABLED =
    buildConf("autoCompact.nonBlindAppend.enabled")
      .internal()
      .doc(
        s"""When enabled, Auto Compaction is only triggered by non-blind-append write
           |transaction.""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val DELTA_AUTO_COMPACT_MAX_NUM_MODIFIED_PARTITIONS =
    buildConf("autoCompact.maxNumModifiedPartitions")
      .internal()
      .doc(
        s"""The maximum number of partition can be selected for Auto Compaction when
           | Auto Compaction runs on modified partition is enabled.""".stripMargin)
      .intConf
      .checkValue(_ > 0, "The value of maxNumModifiedPartitions should be positive.")
      .createWithDefault(128)

  val DELTA_AUTO_COMPACT_RESERVE_PARTITIONS_ENABLED =
    buildConf("autoCompact.reservePartitions.enabled")
      .internal()
      .doc(
        s"""When enabled, each Auto Compact thread reserves its target partitions and skips the
           |partitions that are under Auto Compaction by another thread
           |concurrently.""".stripMargin)
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

  val DELTA_MAX_RETRY_COMMIT_ATTEMPTS =
    buildConf("maxCommitAttempts")
      .internal()
      .doc("The maximum number of commit attempts we will try for a single commit before failing")
      .intConf
      .checkValue(_ >= 0, "maxCommitAttempts has to be positive")
      .createWithDefault(10000000)

  val DELTA_MAX_NON_CONFLICT_RETRY_COMMIT_ATTEMPTS =
    buildConf("maxNonConflictCommitAttempts")
      .internal()
      .doc("The maximum number of non-conflict commit attempts we will try for a single commit " +
        "before failing")
      .intConf
      .checkValue(_ >= 0, "maxNonConflictCommitAttempts has to be positive")
      .createWithDefault(10)

  val FEATURE_ENABLEMENT_CONFLICT_RESOLUTION_ENABLED =
    buildConf("featureEnablement.conflictResolution.enabled")
      .internal()
      .doc(
        """Controls whether we attempt to resolve feature enablement with allowlist.
          |This is only intended to be used as a kill switch.""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_PROTOCOL_DEFAULT_WRITER_VERSION =
    buildConf("properties.defaults.minWriterVersion")
      .doc("The default writer protocol version to create new tables with, unless a feature " +
        "that requires a higher version for correctness is enabled.")
      .intConf
      .checkValues(Set(1, 2, 3, 4, 5, 6, 7))
      .createWithDefault(2)

  val DELTA_PROTOCOL_DEFAULT_READER_VERSION =
    buildConf("properties.defaults.minReaderVersion")
      .doc("The default reader protocol version to create new tables with, unless a feature " +
        "that requires a higher version for correctness is enabled.")
      .intConf
      .checkValues(Set(1, 2, 3))
      .createWithDefault(1)

  val TABLE_FEATURES_TEST_FEATURES_ENABLED =
    buildConf("tableFeatures.testFeatures.enabled")
      .internal()
      .doc("Controls whether test features are enabled in testing mode. " +
        "This config is only used for testing purposes. ")
      .booleanConf
      .createWithDefault(true)

  val UNSUPPORTED_TESTING_FEATURES_ENABLED =
    buildConf("tableFeatures.dev.unsupportedTableFeatures.enabled")
      .internal()
      .doc(
        """When turned on, it emulates the existence of unsupported features by the client.
          |This config is only used for testing purposes.""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val ALLOW_METADATA_CLEANUP_WHEN_ALL_PROTOCOLS_SUPPORTED =
    buildConf("tableFeatures.allowMetadataCleanupWhenAllProtocolsSupported")
      .internal()
      .doc(
        """Whether to perform protocol validation when the client is unable to clean
          |up to 'delta.requireCheckpointProtectionBeforeVersion'.""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val ALLOW_METADATA_CLEANUP_CHECKPOINT_EXISTENCE_CHECK_DISABLED =
    buildConf("tableFeatures.dev.allowMetadataCleanupCheckpointExistenceCheck.disabled")
      .internal()
      .doc(
        """Whether to disable the checkpoint check at the cleanup boundary when performing
          |the CheckpointProtectionTableFeature validations.
          |This is only used for testing purposes.'.""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val FAST_DROP_FEATURE_ENABLED =
    buildConf("tableFeatures.fastDropFeature.enabled")
      .internal()
      .doc(
        """Whether to allow dropping features with the fast drop feature feature
          |functionality.""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val FAST_DROP_FEATURE_DV_DISCOVERY_IN_VACUUM_DISABLED =
    buildConf("tableFeatures.dev.fastDropFeature.DVDiscoveryInVacuum.disabled")
      .internal()
      .doc(
        """Whether to allow DV discovery in Vacuum.
          |This is config is only intended for testing purposes.""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val FAST_DROP_FEATURE_GENERATE_DV_TOMBSTONES =
    buildConf("tableFeatures.fastDropFeature.generateDVTombstones.enabled")
      .internal()
      .doc(
        """Whether to generate DV tombstones when dropping deletion vectors.
          |These make sure deletion vector files won't accidentally be vacuumed by clients
          |that do not support DVs.""".stripMargin)
      .booleanConf
      .createWithDefaultFunction(() => SQLConf.get.getConf(DeltaSQLConf.FAST_DROP_FEATURE_ENABLED))

  val FAST_DROP_FEATURE_DV_TOMBSTONE_COUNT_THRESHOLD =
    buildConf("tableFeatures.fastDropFeature.dvTombstoneCountThreshold")
      .doc(
        """The maximum number of DV tombstones we are allowed store to memory when dropping
          |deletion vectors. When the resulting number of DV tombstones is higher, we use
          |a special commit for large outputs. This does not materialize results to memory
          |but does not retry in case of a conflict.""".stripMargin)
      .intConf
      .checkValue(_ >= 0, "DVTombstoneCountThreshold must not be negative.")
      .createWithDefault(10000)

  val FAST_DROP_FEATURE_STREAMING_ALWAYS_VALIDATE_PROTOCOL =
    buildConf("tableFeatures.fastDropFeature.alwaysValidateProtocolInStreaming.enabled")
      .internal()
      .doc(
        """Whether to validate the protocol when starting a stream from arbitrary
          |versions.""".stripMargin)
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

  val DELTA_REPLACE_COLUMNS_SAFE =
    buildConf("alter.replaceColumns.safe.enabled")
      .internal()
      .doc("Prevents an ALTER TABLE REPLACE COLUMNS method from dropping all columns, which " +
        "leads to losing all data. It will only allow safe, unambiguous column changes.")
      .booleanConf
      .createWithDefault(true)

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

  val DELTA_VACUUM_RETENTION_WINDOW_IGNORE_ENABLED =
    buildConf("vacuum.retentionWindowIgnore.enabled")
      .internal()
      .doc("When set, retention window as part of Vacuum will be ignored unless the value is 0")
      .booleanConf
      .createWithDefault(true)

  val DELTA_HISTORY_MANAGER_THREAD_POOL_SIZE =
    buildConf("history.threadPoolSize")
      .internal()
      .doc("The size of the thread pool used for search during DeltaHistory operations. " +
        "This configuration is only used when the feature inCommitTimestamps is enabled.")
      .intConf
      .checkValue(_ > 0, "history.threadPoolSize must be positive")
      .createWithDefault(10)

  val ENFORCE_TIME_TRAVEL_WITHIN_DELETED_FILE_RETENTION_DURATION =
    buildConf("vacuum.enforceTimeTravelWithinDeletedFileRetentionDuration")
      .internal()
      .doc("Enforces time travel within delta.deletedFileRetentionDuration.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_VACUUM_LOGGING_ENABLED =
    buildConf("vacuum.logging.enabled")
      .doc("Whether to log vacuum information into the Delta transaction log." +
        " Users should only set this config to 'true' when the underlying file system safely" +
        " supports concurrent writes.")
      .booleanConf
      .createOptional

  val LITE_VACUUM_ENABLED =
    buildConf("vacuum.lite.enabled")
      .doc("Allows Vacuum to be run in Lite mode")
      .booleanConf
      .createWithDefault(false)

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
        "is controlled 'spark.databricks.delta.vacuum.parallelDelete.parallelism'.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_VACUUM_PARALLEL_DELETE_PARALLELISM =
    buildConf("vacuum.parallelDelete.parallelism")
      .doc("Sets the number of partitions to use for parallel deletes. If not set, defaults to " +
        "spark.sql.shuffle.partitions.")
      .intConf
      .checkValue(_ > 0, "parallelDelete.parallelism must be positive")
      .createOptional

  val ENFORCE_DELETED_FILE_AND_LOG_RETENTION_DURATION_COMPATIBILITY =
    buildConf("vacuum.enforceDeletedFileAndLogRetentionDurationCompatibility")
      .internal()
      .doc("Throws an error if log retention duration is less than deletedFileRetentionDuration")
      .booleanConf
      .createWithDefault(true)

  val DELTA_SCHEMA_AUTO_MIGRATE =
    buildConf("schema.autoMerge.enabled")
      .doc("If true, enables schema merging on appends and on overwrites.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_MERGE_SCHEMA_EVOLUTION_FIX_NESTED_STRUCT_ALIGNMENT =
    buildConf("schemaEvolution.merge.fixNestedStructAlignment")
      .internal()
      .doc("Internal flag covering a fix for a regression in schema evolution inside nested " +
        "structs in MERGE. Disabling this fix may cause MERGE operations to fail when a new " +
        "field is added to a struct that is omitted in at least one MATCHED clause.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_MERGE_PRESERVE_NULL_SOURCE_STRUCTS =
    buildConf("merge.preserveNullSourceStructs")
      .internal()
      .doc(
        """Fixes the null expansion issue by preserving NULL structs in MERGE operations. When set
          |to true, a NULL struct in the source will be preserved as NULL in the target after MERGE,
          |rather than being incorrectly expanded to a struct with NULL fields. When set to false,
          |NULL structs are expanded. This fix addresses null expansion caused by (1) struct type
          |cast, and (2) expanding UPDATE SET * to leaf-level actions in schema evolution (when
          |`spark.databricks.delta.merge.preserveNullSourceStructs.updateStar` is also enabled).
          |Note: The fix for struct type cast also fixes the null expansion issue in UPDATE queries
          |and streaming inserts with struct type cast.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(DeltaUtils.isTesting)

  val DELTA_MERGE_PRESERVE_NULL_SOURCE_STRUCTS_UPDATE_STAR =
    buildConf("merge.preserveNullSourceStructs.updateStar")
      .internal()
      .doc("""Fixes the null expansion issue in MERGE with UPDATE SET * actions in schema evolution.
             |When set to true, and `spark.databricks.delta.merge.preserveNullSourceStructs` is also
             |true, a NULL struct in the source will be preserved as NULL in the target after MERGE,
             |rather than being incorrectly expanded to a struct with NULL fields. Otherwise, NULL
             |structs are expanded.""".stripMargin)
      .fallbackConf(DELTA_MERGE_PRESERVE_NULL_SOURCE_STRUCTS)

  val DELTA_INSERT_PRESERVE_NULL_SOURCE_STRUCTS =
    buildConf("insert.preserveNullSourceStructs")
      .internal()
      .doc(
        """Fixes the null expansion issue by preserving NULL structs in INSERT operations. When set
          |to true, a NULL struct in the source will be preserved as NULL in the target after
          |INSERT, rather than being incorrectly expanded to a struct with NULL fields. When set to
          |false, NULL structs are expanded. This fix addresses null expansion caused by struct
          |type cast during INSERT operations.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(DeltaUtils.isTesting)

  val DELTA_SCHEMA_TYPE_CHECK =
    buildConf("schema.typeCheck.enabled")
      .doc(
        """Enable the data type check when updating the table schema. Disabling this flag may
          | allow users to create unsupported Delta tables and should only be used when trying to
          | read/write legacy tables.""".stripMargin)
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_SCHEMA_REMOVE_SPARK_INTERNAL_METADATA =
    buildConf("schema.removeSparkInternalMetadata")
      .doc(
        """Whether to remove leaked Spark's internal metadata from the table schema before returning
          |to Spark. These internal metadata might be stored unintentionally in tables created by
          |old Spark versions""".stripMargin)
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_UPDATE_CATALOG_ENABLED =
    buildConf("catalog.update.enabled")
      .internal()
      .doc("When enabled, we will cache the schema of the Delta table and the table properties " +
        "in the external catalog, e.g. the Hive MetaStore.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_UPDATE_CATALOG_THREAD_POOL_SIZE =
    buildStaticConf("catalog.update.threadPoolSize")
      .internal()
      .doc("The size of the thread pool for updating the external catalog.")
      .intConf
      .checkValue(_ > 0, "threadPoolSize must be positive")
      .createWithDefault(20)

  val COORDINATED_COMMITS_GET_COMMITS_THREAD_POOL_SIZE =
    buildStaticConf("coordinatedCommits.getCommits.threadPoolSize")
      .internal()
      .doc("The size of the thread pool for listing files from the commit-coordinator.")
      .intConf
      .checkValue(_ > 0, "threadPoolSize must be positive")
      .createWithDefault(5)

  val COORDINATED_COMMITS_IGNORE_MISSING_COORDINATOR_IMPLEMENTATION =
    buildConf("coordinatedCommits.ignoreMissingCoordinatorImplementation")
      .internal()
      .doc("When enabled, reads will not fail if the commit coordinator implementation " +
        "is missing. Writes will still fail and reads will just rely on backfilled commits. " +
        "This also means that reads can be stale.")
      .booleanConf
      .createWithDefault(true)

  val REMOVE_EXISTS_DEFAULT_FROM_SCHEMA =
    buildConf("schema.removeExistsDefault")
      .internal()
      .doc("When enabled, do not store the 'EXISTS_DEFAULT' metadata key when a table with a " +
        "default value is created and this table does not re-use existing data files." +
        "'EXISTS_DEFAULT' holds values that are used in Spark for existing rows when a new column" +
        "with a default value is added to a table. Since we do not support adding columns with a" +
        "default value in Delta, this metadata key can be omitted, except in cases like when" +
        "we convert a table to Delta that does actually require 'EXISTS_DEFAULT'.")
      .booleanConf
      .createWithDefault(true)

  val HMS_FORCE_ALTER_TABLE_DATA_SCHEMA =
    buildConf("hms.schema.forceAlterTableDataSchema")
      .internal()
      .doc(
        """
          | This conf fixes the schema in tableCatalog object and force an alter table
          | schema command after upload the schema. As in spark project the schema is removed
          | because delta is not a valid serDe configuration. This is a problem known only to HMS.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  //////////////////////////////////////////////
  // DynamoDB Commit Coordinator-specific configs
  /////////////////////////////////////////////

  val COORDINATED_COMMITS_DDB_AWS_CREDENTIALS_PROVIDER_NAME =
    buildConf("coordinatedCommits.commitCoordinator.dynamodb.awsCredentialsProviderName")
      .internal()
      .doc("The fully qualified class name of the AWS credentials provider to use for " +
        "interacting with DynamoDB in the DynamoDB Commit Coordinator Client. e.g. " +
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain.")
      .stringConf
      .createWithDefault("com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

  val COORDINATED_COMMITS_DDB_SKIP_PATH_CHECK =
    buildConf("coordinatedCommits.commitCoordinator.dynamodb.skipPathCheckEnabled")
      .internal()
      .doc("When enabled, the DynamoDB Commit Coordinator will not enforce that the table path " +
        "of the current Delta table matches the stored in the corresponding DynamoDB table. This " +
        "should only be used when the observed table path for the same physical table varies " +
        "depending on how it is accessed (e.g. abfs://path1 vs abfss://path1). Leaving this " +
        "enabled can be dangerous as every physical copy of a Delta table with try to write to" +
        " the same DynamoDB table.")
      .booleanConf
      .createWithDefault(false)

  val COORDINATED_COMMITS_DDB_READ_CAPACITY_UNITS =
    buildConf("coordinatedCommits.commitCoordinator.dynamodb.readCapacityUnits")
      .internal()
      .doc("Controls the provisioned read capacity units for the DynamoDB table backing the " +
        "DynamoDB Commit Coordinator. This configuration is only used when the DynamoDB table " +
        "is first provisioned and cannot be used configure an existing table.")
      .intConf
      .createWithDefault(5)

  val COORDINATED_COMMITS_DDB_WRITE_CAPACITY_UNITS =
    buildConf("coordinatedCommits.commitCoordinator.dynamodb.writeCapacityUnits")
      .internal()
      .doc("Controls the provisioned write capacity units for the DynamoDB table backing the " +
        "DynamoDB Commit Coordinator. This configuration is only used when the DynamoDB table " +
        "is first provisioned and cannot be used configure an existing table.")
      .intConf
      .createWithDefault(5)

  //////////////////////////////////////////////
  // DynamoDB Commit Coordinator-specific configs end
  /////////////////////////////////////////////

  val IN_COMMIT_TIMESTAMP_RETAIN_ENABLEMENT_INFO_FIX_ENABLED =
    buildConf("inCommitTimestamp.retainEnablementInfoFix.enabled")
      .internal()
      .doc("When disabled, Delta can end up dropping " +
        s"inCommitTimestampEnablementVersion and inCommitTimestampEnablementTimestamp " +
        s"during a REPLACE or CLONE command. This accidental removal of these " +
        s"properties can result in failures on time travel queries.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_UPDATE_CATALOG_LONG_FIELD_TRUNCATION_THRESHOLD =
    buildConf("catalog.update.longFieldTruncationThreshold")
      .internal()
      .doc(
        "When syncing table schema to the catalog, Delta will truncate the whole schema " +
        "if any field is longer than this threshold.")
      .longConf
      .createWithDefault(4000)

  val DELTA_ASSUMES_DROP_CONSTRAINT_IF_EXISTS =
    buildConf("constraints.assumesDropIfExists.enabled")
      .doc("""If true, DROP CONSTRAINT quietly drops nonexistent constraints even without
             |IF EXISTS.
           """)
      .booleanConf
      .createWithDefault(false)

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
      .checkValue(_ >= 0, "Staleness limit cannot be negative")
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

  val DELTA_MERGE_ANALYSIS_BATCH_RESOLUTION =
    buildConf("merge.analysis.batchActionResolution.enabled")
      .internal()
      .doc(
        """
          | Whether to batch the analysis of all DeltaMergeActions within a clause
          | during merge's analysis resolution.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

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
      .createWithDefault(true)

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

  val MERGE_FAIL_IF_SOURCE_CHANGED =
    buildConf("merge.failIfSourceChanged")
      .internal()
      .doc(
        """
          |When enabled, MERGE will fail if it detects that the source dataframe was changed.
          |This can be triggered as a result of modified input data or the use of nondeterministic
          |query plans. The detection is best-effort.
      """.stripMargin)
      .booleanConf
      .createWithDefault(false)

  final object MergeMaterializeSource {
    // See value explanations in the doc below.
    final val NONE = "none"
    final val ALL = "all"
    final val AUTO = "auto"

    final val list = Set(NONE, ALL, AUTO)
  }

  val MERGE_MATERIALIZE_SOURCE =
    buildConf("merge.materializeSource")
      .internal()
      .doc("When to materialize the source plan during MERGE execution. " +
        "The value 'none' means source will never be materialized. " +
        "The value 'all' means source will always be materialized. " +
        "The value 'auto' means sources will not be materialized when they are certain to be " +
        "deterministic."
      )
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValues(MergeMaterializeSource.list)
      .createWithDefault(MergeMaterializeSource.AUTO)

  val MERGE_FORCE_SOURCE_MATERIALIZATION_WITH_UNREADABLE_FILES =
    buildConf("merge.forceSourceMaterializationWithUnreadableFilesConfig")
      .internal()
      .doc(
        s"""
           |When set to true, merge command will force source materialization if Spark configs
           |${SQLConf.IGNORE_CORRUPT_FILES.key}, ${SQLConf.IGNORE_MISSING_FILES.key} or
           |file source read options ${FileSourceOptions.IGNORE_CORRUPT_FILES}
           |${FileSourceOptions.IGNORE_MISSING_FILES} are enabled on the source.
           |This is done so to prevent irrecoverable data loss or unexpected results.
           |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val MERGE_MATERIALIZE_CACHED_SOURCE =
    buildConf("merge.materializeCachedSource")
      .internal()
      .doc(
        """
          |When enabled, materialize the source in MERGE if it is cached (e.g. via df.cache()). This
          |prevents incorrect results due to query caching not pinning the version of cached Delta
          |tables.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val MERGE_FAIL_SOURCE_CACHED_AFTER_MATERIALIZATION =
    buildConf("merge.failSourceCachedAfterMaterialization")
      .internal()
      .doc(
        """
          |Enables a check that fails the MERGE operation if the source was cached (using
          |df.cache()) after the source materialization phase. Query caching doesn't pin the version
          |of Delta tables and we should materialize cached source plans. In rare cases, the source
          |might get cached after the decision to materialize, which could lead to incorrect results
          |if we let the operation succeed.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL =
    buildConf("merge.materializeSource.rddStorageLevel")
      .internal()
      .doc("What StorageLevel to use to persist the source RDD. Note: will always use disk.")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue( v =>
        try {
          StorageLevel.fromString(v).isInstanceOf[StorageLevel]
        } catch {
          case _: IllegalArgumentException => true
        },
        """"spark.databricks.delta.merge.materializeSource.rddStorageLevel" """ +
          "must be a valid StorageLevel")
      .createWithDefault("DISK_ONLY")

  val MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL_FIRST_RETRY =
    buildConf("merge.materializeSource.rddStorageLevelFirstRetry")
      .internal()
      .doc("What StorageLevel to use to persist the source RDD when MERGE is retried the first" +
        "time. Note: will always use disk.")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue( v =>
        try {
          StorageLevel.fromString(v).isInstanceOf[StorageLevel]
        } catch {
          case _: IllegalArgumentException => true
        },
        """"spark.databricks.delta.merge.materializeSource.rddStorageLevelFirstRetry" """ +
          "must be a valid StorageLevel")
      .createWithDefault("DISK_ONLY_2")

  val MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL_RETRY =
    buildConf("merge.materializeSource.rddStorageLevelRetry")
      .internal()
      .doc("What StorageLevel to use to persist the source RDD when MERGE is retried after the " +
        "first retry. The storage level to use for the first retry can be configured using" +
        """"spark.databricks.delta.merge.materializeSource.rddStorageLevelFirstRetry" """ +
        "Note: will always use disk.")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue( v =>
        try {
          StorageLevel.fromString(v).isInstanceOf[StorageLevel]
        } catch {
          case _: IllegalArgumentException => true
        },
        """"spark.databricks.delta.merge.materializeSource.rddStorageLevelRetry" """ +
          "must be a valid StorageLevel")
      .createWithDefault("DISK_ONLY_3")

  val MERGE_MATERIALIZE_SOURCE_MAX_ATTEMPTS =
    buildStaticConf("merge.materializeSource.maxAttempts")
      .doc("How many times to try MERGE in case of lost RDD materialized source data")
      .intConf
      .createWithDefault(4)

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

  val CHECKPOINT_SCHEMA_WRITE_THRESHOLD_LENGTH =
    buildConf("checkpointSchema.writeThresholdLength")
      .internal()
      .doc("Checkpoint schema larger than this threshold won't be written to the last checkpoint" +
        " file")
      .intConf
      .createWithDefault(20000)

  val LAST_CHECKPOINT_CHECKSUM_ENABLED =
    buildConf("lastCheckpoint.checksum.enabled")
      .internal()
      .doc("Controls whether to write the checksum while writing the LAST_CHECKPOINT file and" +
        " whether to validate it while reading the LAST_CHECKPOINT file")
      .booleanConf
      .createWithDefault(true)

  val SUPPRESS_OPTIONAL_LAST_CHECKPOINT_FIELDS =
      buildConf("lastCheckpoint.suppressOptionalFields")
      .internal()
      .doc("If set, the LAST_CHECKPOINT file will contain only version, size, and parts fields. " +
          "For compatibility with broken third-party connectors that choke on unrecognized fields.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_CHECKPOINT_PART_SIZE =
    buildConf("checkpoint.partSize")
        .internal()
        .doc("The limit at which we will start parallelizing the checkpoint. We will attempt to " +
                 "write a maximum of this many actions per checkpoint file.")
        .longConf
        .checkValue(_ > 0, "partSize has to be positive")
        .createOptional

  /////////////////////////////////
  // File Materialization Tracker
  /////////////////////////////////

  val DELTA_COMMAND_FILE_MATERIALIZATION_TRACKING_ENABLED =
    buildConf("command.fileMaterializationLimit.enabled")
      .internal()
      .doc(
        """
          |When enabled, tracks the file metadata materialized on the driver and restricts the
          |number of files materialized on the driver to be within the global file
          |materialization limit.
       """.stripMargin
      )
      .booleanConf
      .createWithDefault(true)

  val DELTA_COMMAND_FILE_MATERIALIZATION_LIMIT =
    buildStaticConf("command.fileMaterializationLimit.softMax")
      .internal()
      .doc(
        s"""
           |The soft limit for the total number of file metadata that can be materialized at once on
           |the driver. This config will take effect only when
           |${DELTA_COMMAND_FILE_MATERIALIZATION_TRACKING_ENABLED.key} is enabled.
        """.stripMargin
      )
      .intConf
      .checkValue(_ >= 0, "'command.fileMaterializationLimit.softMax' must be positive")
      .createWithDefault(10000000)

  ////////////////////////
  // BACKFILL
  ////////////////////////

  val DELTA_ROW_TRACKING_BACKFILL_ENABLED =
    buildConf("rowTracking.backfill.enabled")
      .internal()
      .doc("Whether Row Tracking backfill can be performed.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_ROW_TRACKING_BACKFILL_MAX_NUM_FILES_PER_COMMIT =
    buildConf("rowTracking.backfill.maxNumFiles")
      .internal()
      .doc("The maximum number of files to include in a single commit when running " +
        "RowTrackingBackfillCommand. The default maximum aims to keep every " +
        "delta log entry below 100mb.")
      .intConf
      .checkValue(_ > 0, "'backfill.maxNumFiles' must be positive.")
      .createWithDefault(22000)

  val DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT =
    buildConf("backfill.maxNumFiles")
      .internal()
      .doc("The maximum number of files to include in a single commit when running " +
        "BackfillCommand. The default maximum aims to keep every " +
        "delta log entry below 100mb.")
      .fallbackConf(DELTA_ROW_TRACKING_BACKFILL_MAX_NUM_FILES_PER_COMMIT)

  val DELTA_BACKFILL_MAX_NUM_FILES_FACTOR =
    buildConf("backfill.maxNumFilesFactor")
      .internal()
      .doc(
        """The factor used to compute the maximum number of files to backfill.
          |The maximum number of files to compute in backfill is computed as
          |number of files in table * factor.""".stripMargin)
      .doubleConf
      .checkValue(_ > 0, "'backfill.maxNumFilesFactor' must be greater than zero.")
      .createWithDefault(3)

  val DELTA_ROW_TRACKING_IGNORE_SUSPENSION =
    buildConf("rowTracking.ignoreSuspension")
      .internal()
      .doc(
        """Controls whether to ignore `delta.rowTrackingSuspended` property.
          |This is a testing only config.""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  ////////////////////////////////////
  // Checkpoint V2 Specific Configs
  ////////////////////////////////////

  val CHECKPOINT_V2_DRIVER_THREADPOOL_PARALLELISM =
    buildStaticConf("checkpointV2.threadpool.size")
      .doc("The size of the threadpool for fetching CheckpointMetadata and SidecarFiles from a" +
        " checkpoint.")
      .internal()
      .intConf
      .createWithDefault(32)

  val CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT =
    buildConf("checkpointV2.topLevelFileFormat")
      .internal()
      .doc(
        """
          |The file format to use for the top level checkpoint file in V2 Checkpoints.
          | This can be set to either json or parquet. The appropriate format will be
          | picked automatically if this config is not specified.
          |""".stripMargin)
      .stringConf
      .checkValues(Set("json", "parquet"))
      .createOptional

  // This is temporary conf to make sure v2 checkpoints are not used by anyone other than devs as
  // the feature is not fully ready.
  val EXPOSE_CHECKPOINT_V2_TABLE_FEATURE_FOR_TESTING =
    buildConf("checkpointV2.exposeTableFeatureForTesting")
      .internal()
      .doc(
        """
          |This conf controls whether v2 checkpoints table feature is exposed or not. Note that
          | v2 checkpoints are in development and this should config should be used only for
          | testing/benchmarking.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val LAST_CHECKPOINT_NON_FILE_ACTIONS_THRESHOLD =
    buildConf("lastCheckpoint.nonFileActions.threshold")
      .internal()
      .doc("""
          |Threshold for total number of non file-actions to store in the last_checkpoint
          | corresponding to the checkpoint v2.
          |""".stripMargin)
      .intConf
      .createWithDefault(30)

  val STATS_AS_STRUCT_IN_CHECKPOINT_FORCE_DISABLED =
    buildConf("statsAsStructInCheckpoint.forcedDisabled")
      .internal()
      .doc("""
          |Force disables storing statistics as struct in the checkpoint.
          |Note that should only be used as a kill switch.
          |This functionality should normally be controlled using the delta config
          |'checkpoint.writeStatsAsStruct'.
          |""".stripMargin)
      .booleanConf
      .createOptional

  val LAST_CHECKPOINT_SIDECARS_THRESHOLD =
    buildConf("lastCheckpoint.sidecars.threshold")
      .internal()
      .doc("""
          |Threshold for total number of sidecar files to store in the last_checkpoint
          | corresponding to the checkpoint v2.
          |""".stripMargin)
      .intConf
      .createWithDefault(30)

  val USE_CHECKPOINT_SCHEMA_FROM_CHECKPOINT_METADATA =
    buildConf("checkpointSchema.useFromCheckpointMetadata")
      .internal()
      .doc("If enabled, use checkpoint schema from checkpoint metadata file instead of reading it" +
        " from the checkpoint file")
      .booleanConf
      .createWithDefault(true)

  val DELTA_WRITE_CHECKSUM_ENABLED =
    buildConf("writeChecksumFile.enabled")
      .doc("Whether the checksum file can be written.")
      .booleanConf
      .createWithDefault(true)

  private val FORCED_CHECKSUM_VALIDATION_INTERVAL_DEFAULT = 400
  val FORCED_CHECKSUM_VALIDATION_INTERVAL =
    buildConf("versionChecksum.forcedValidationInterval")
      .internal()
      .doc("The number of commits since the last checkpoint at which we " +
        "should force validation of the version checksum. This is done before " +
        "a commit to block further writes in case of checksum mismatch." +
        "Set to -1 to disable, set to 0 to validate on every commit. " +
        "The validation will be skipped if the checkpoint was created " +
        "within the time gap specified by versionChecksum.forcedValidationMinTimeIntevalMinutes.")
      .intConf
      .createWithDefault(FORCED_CHECKSUM_VALIDATION_INTERVAL_DEFAULT)

  val FORCED_CHECKSUM_VALIDATION_MIN_TIME_INTERVAL_MINUTES =
    buildConf("versionChecksum.forcedValidationMinTimeIntevalMinutes")
      .internal()
      .doc("The minimum time gap in minutes between the checkpoint creation time and " +
        "current time for forced checksum validation. If the checkpoint was created " +
        "within this time gap, forced validation is skipped even if the number of " +
        "commits since the checkpoint exceeds the forcedValidationInterval threshold. " +
        "For fast moving tables, the checkpoint can lag much behind " +
        "versionChecksum.forcedValidationInterval. This helps us avoid slowing " +
        "them down. Set to 0 to disable this optimization.")
      .intConf
      .checkValue(_ >= 0,
        "'versionChecksum.forcedValidationMinTimeIntevalMinutes' must be non-negative.")
      .createWithDefault(12*60) // 12 hours

  val INCREMENTAL_COMMIT_ENABLED =
    buildConf("incremental.commit.enabled")
      .internal()
      .doc("If true, Delta will incrementally compute the content of the commit checksum " +
        "file, which avoids the full state reconstruction that would otherwise be required.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_CHECKSUM_MISMATCH_IS_FATAL =
    buildConf("checksum.mismatch.fatal")
      .internal()
      .doc(
        """If true, throws a fatal error when the recreated Delta State doesn't
          |match committed checksum file.
        """)
      .booleanConf
      .createWithDefault(true)

  val INCREMENTAL_COMMIT_VERIFY =
    buildConf("incremental.commit.verify")
      .internal()
      .doc("If true, Delta commit will validate the commit checksum file content before and " +
        "after each incremental commit. Note that this requires two full state reconstructions.")
      .booleanConf
      .createWithDefault(false)

  // This config is effective only in unit tests.
  val INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS =
    buildConf("incremental.commit.forceVerifyInTests")
      .internal()
      .doc("If true, Delta commit will validate the commit checksum file content before and " +
        "after each incremental commit as part of Unit Tests. Note that this overrides any " +
        s"behaviour from ${INCREMENTAL_COMMIT_VERIFY.key} config.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_WRITE_SET_TRANSACTIONS_IN_CRC =
    buildConf("setTransactionsInCrc.writeOnCommit")
      .internal()
      .doc("When enabled, each commit will incrementally compute and cache all SetTransaction" +
        " actions in the .crc file. Note that this only happens when incremental commits" +
        s" are enabled (${INCREMENTAL_COMMIT_ENABLED.key})")
      .booleanConf
      .createWithDefault(true)

  val DELTA_MAX_SET_TRANSACTIONS_IN_CRC =
    buildConf("setTransactionsInCrc.maxAllowed")
      .internal()
      .doc("Threshold of the number of SetTransaction actions below which this optimization" +
        " should be enabled")
      .longConf
      .createWithDefault(100)

  val DELTA_MAX_DOMAIN_METADATAS_IN_CRC =
    buildConf("domainMetadatasInCrc.maxAllowed")
      .internal()
      .doc("Threshold of the number of DomainMetadata actions below which this optimization" +
        " should be enabled")
      .longConf
      .createWithDefault(10)

  val DELTA_ALL_FILES_IN_CRC_THRESHOLD_FILES =
    buildConf("allFilesInCrc.thresholdNumFiles")
      .internal()
      .doc("Threshold of the number of AddFiles below which AddFiles will be added to CRC.")
      .intConf
      .createWithDefault(50)

  val DELTA_ALL_FILES_IN_CRC_ENABLED =
    buildConf("allFilesInCrc.enabled")
      .internal()
      .doc("When enabled, [[Snapshot.allFiles]] will be stored in the .crc file when the " +
        "length is less than the threshold specified by " +
        s"${DELTA_ALL_FILES_IN_CRC_THRESHOLD_FILES.key}. " +
        "Note that this config only takes effect when incremental commits are enabled " +
        s"(${INCREMENTAL_COMMIT_ENABLED.key})."
      )
      .booleanConf
      .createWithDefault(true)

  val DELTA_ALL_FILES_IN_CRC_VERIFICATION_MODE_ENABLED =
    buildConf("allFilesInCrc.verificationMode.enabled")
      .internal()
      .doc(s"This will be effective only if ${DELTA_ALL_FILES_IN_CRC_ENABLED.key} is set. When" +
        " enabled, We will have additional verification of the incrementally computed state by" +
        " doing an actual state reconstruction on every commit.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_ALL_FILES_IN_CRC_FORCE_VERIFICATION_MODE_FOR_NON_UTC_ENABLED =
    buildConf("allFilesInCrc.verificationMode.forceOnNonUTC.enabled")
      .internal()
      .doc(s"This will be effective only if " +
        s"${DELTA_ALL_FILES_IN_CRC_VERIFICATION_MODE_ENABLED.key} is not set. When enabled, we " +
        s"will force verification of the incrementally computed state by doing an actual state " +
        s"reconstruction on every commit for tables that are not using UTC timezone.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_ALL_FILES_IN_CRC_THRESHOLD_INDEXED_COLS =
    buildConf("allFilesInCrc.thresholdIndexedCols")
      .internal()
      .doc("If the delta table is configured to collect stats on more columns than this" +
        " threshold, then disable storage of `[[Snapshot.allFiles]]` in the .crc file.")
      .intConf
      .createOptional

  val USE_PROTOCOL_AND_METADATA_FROM_CHECKSUM_ENABLED =
    buildConf("readProtocolAndMetadataFromChecksum.enabled")
      .internal()
      .doc("If enabled, delta log snapshot will read the protocol, metadata, and ICT " +
        "(if applicable) from the checksum file and use those to avoid a spark job over the " +
        "checkpoint for the two rows of protocol and metadata")
      .booleanConf
      .createWithDefault(true)

  val DELTA_CHECKSUM_DV_METRICS_ENABLED =
    buildConf("checksumDVMetrics.enabled")
      .internal()
      .doc(s"""When enabled, each delta transaction includes vector metrics in the checksum.
              |Only applies to tables that use Deletion Vectors."""
        .stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_DELETED_RECORD_COUNTS_HISTOGRAM_ENABLED =
    buildConf("checksumDeletedRecordCountsHistogramMetrics.enabled")
      .internal()
      .doc(s"""When enabled, each delta transaction includes in the checksum the deleted
              |record count distribution histogram for all the files. To enable this feature
              |${DELTA_CHECKSUM_DV_METRICS_ENABLED.key} needs to be enabled as well. Only
              |applies to tables that use Deletion Vectors.""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_CHECKPOINT_THROW_EXCEPTION_WHEN_FAILED =
      buildConf("checkpoint.exceptionThrowing.enabled")
        .internal()
      .doc("Throw an error if checkpoint is failed. This flag is intentionally used for " +
          "testing purpose to catch the checkpoint issues proactively. In production, we " +
          "should not set this flag to be true because successful commit should return " +
          "success to client regardless of the checkpoint result without throwing.")
      .booleanConf
      .createWithDefault(false)

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

  /**
   * Internal config to bypass the check that ensures a table doesn't contain any unsupported type
   * change when reading it. Meant as a mitigation in case the check incorrectly flags valid cases.
   */
  val DELTA_TYPE_WIDENING_BYPASS_UNSUPPORTED_TYPE_CHANGE_CHECK =
    buildConf("typeWidening.bypassUnsupportedTypeChangeCheck")
      .internal()
      .doc("""
           | Disables check that ensures a table doesn't contain any unsupported type change when
           | reading it.
           |""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val DELTA_ALLOW_TYPE_WIDENING_STREAMING_SOURCE =
    buildConf("typeWidening.allowTypeChangeStreamingDeltaSource")
      .doc("Accept incoming widening type changes when streaming from a Delta source.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  object AllowAutomaticWideningMode extends Enumeration {
    val NEVER, SAME_FAMILY_TYPE, ALWAYS = Value

    def fromConf(conf: SQLConf): Value =
      withName(conf.getConf(DELTA_ALLOW_AUTOMATIC_WIDENING))

    def default: Value =
      withName(DELTA_ALLOW_AUTOMATIC_WIDENING.defaultValueString)
  }

  val DELTA_ALLOW_AUTOMATIC_WIDENING =
    buildConf("typeWidening.allowAutomaticWidening")
      .doc("Controls the scope of enabled widening conversions in automatic schema widening " +
        "during schema evolution. This flag is guarded by the flag 'delta.enableTypeWidening'" +
        "All supported widenings are enabled with 'always' selected, which allows some " +
        "conversions between integer types and floating numbers. The value 'same_family_type' " +
        "was the historical behavior. 'never' allows no widenings.")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(AllowAutomaticWideningMode.values.map(_.toString))
      .createWithDefault(AllowAutomaticWideningMode.ALWAYS.toString)

  val DELTA_TYPE_WIDENING_ENABLE_STREAMING_SCHEMA_TRACKING =
    buildConf("typeWidening.enableStreamingSchemaTracking")
      .doc("Whether to enable schema tracking when streaming from a Delta source that had a " +
        "widening type change applied. This allows blocking the stream on restart until the user " +
        "acknowledges the type change. When disabled, we will not initialize a schema tracking " +
        "log when first detecting a type change and will automatically accept the type change " +
        "instead.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DELTA_TYPE_WIDENING_BYPASS_STREAMING_TYPE_CHANGE_CHECK =
    buildConf("typeWidening.bypassStreamingTypeChangeCheck")
      .doc("Controls the check performed when a type change is detected when streaming from a " +
        "Delta source. This check fails the streaming query in case a type change may impact the " +
        "semantics of the query and requests user intervention.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  /**
   * Internal config to bypass check that prevents applying type changes that are not supported by
   * Iceberg when Uniform is enabled with Iceberg compatibility.
   */
  val DELTA_TYPE_WIDENING_ALLOW_UNSUPPORTED_ICEBERG_TYPE_CHANGES =
    buildConf("typeWidening.allowUnsupportedIcebergTypeChanges")
      .internal()
      .doc(
        """
          |By default, type changes that aren't supported by Iceberg are rejected when Uniform is
          |enabled with Iceberg compatibility. This config allows bypassing this restriction, but
          |reading the affected column with Iceberg clients will likely fail or behave erratically.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(false)

    val DELTA_TYPE_WIDENING_REMOVE_SCHEMA_METADATA =
    buildConf("typeWidening.removeSchemaMetadata")
      .doc("When true, type widening metadata is removed from schemas that are surfaced outside " +
        "of Delta or used for schema comparisons")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_TYPE_WIDENING_ALLOW_INTEGRAL_DECIMAL_COERCION =
    buildConf("typeWidening.allowIntegralDecimalCoercion")
      .doc("When true, the type widening mode `AllTypeWideningToCommonWiderType` " +
        "should allow converting integral types to DecimalType and use decimal " +
        "coercion to find a common wider type with another DecimalType")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_IS_DELTA_TABLE_THROW_ON_ERROR =
    buildConf("isDeltaTable.throwOnError")
      .internal()
      .doc("""
        | If checking the path provided to isDeltaTable (or findDeltaTableRoot) throws an exception,
        | then propagate this exception unless a _delta_log directory is found in an
        | accessible parent.
        | When disabled, such any exception leads to a result indicating that this is not a
        | Delta table.
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

  val DELTA_VACUUM_RELATIVIZE_IGNORE_ERROR =
    buildConf("vacuum.relativize.ignoreError")
      .internal()
      .doc("""
             |When enabled, the error when trying to relativize an absolute path when
             |vacuuming a delta table will be ignored. This usually happens when a table is
             |shallow cloned across FileSystems, such as across buckets or across cloud storage
             |systems. We do not recommend enabling this configuration in production or using it
             |with production datasets.
             |""".stripMargin)
      .booleanConf
      .createWithDefault(false)
  val DELTA_LEGACY_ALLOW_AMBIGUOUS_PATHS =
    buildConf("legacy.allowAmbiguousPathsInCreateTable")
      .internal()
      .doc("""
             |Delta was unintentionally allowing CREATE TABLE queries with both 'delta.`path`'
             |and 'LOCATION path' clauses. In the new version, we will raise an error
             |for this case. This flag is added to allow users to skip the check. When it's set to
             |true and there are two paths in CREATE TABLE, the LOCATION path clause will be
             |ignored like what the old version does.""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val DELTA_WORK_AROUND_COLONS_IN_HADOOP_PATHS =
    buildConf("workAroundColonsInHadoopPaths.enabled")
      .internal()
      .doc("""
             |When enabled, Delta will work around to allow colons in file paths. Normally Hadoop
             |does not support colons in file paths due to ambiguity, but some file systems like
             |S3 allow them.
             |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val REPLACEWHERE_DATACOLUMNS_ENABLED =
    buildConf("replaceWhere.dataColumns.enabled")
      .doc(
        """
          |When enabled, replaceWhere on arbitrary expression and arbitrary columns is enabled.
          |If disabled, it falls back to the old behavior
          |to replace on partition columns only.""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val REPLACEWHERE_METRICS_ENABLED =
    buildConf("replaceWhere.dataColumns.metrics.enabled")
      .internal()
      .doc(
        """
          |When enabled, replaceWhere operations metrics on arbitrary expression and
          |arbitrary columns is enabled. This will not report row level metrics for partitioned
          |tables and tables with no stats.""".stripMargin)
      .booleanConf
      .createWithDefault(true)
  val REPLACEWHERE_CONSTRAINT_CHECK_ENABLED =
    buildConf("replaceWhere.constraintCheck.enabled")
      .doc(
        """
          |When enabled, replaceWhere on arbitrary expression and arbitrary columns will
          |enforce the constraint check to replace the target table only when all the
          |rows in the source dataframe match that constraint.
          |If disabled, it will skip the constraint check and replace with all the rows
          |from the new dataframe.""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val REPLACEWHERE_DATACOLUMNS_WITH_CDF_ENABLED =
    buildConf("replaceWhere.dataColumnsWithCDF.enabled")
      .internal()
      .doc(
        """
          |When enabled, replaceWhere on arbitrary expression and arbitrary columns will produce
          |results for CDF. If disabled, it will fall back to the old behavior.""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val OVERWRITE_REMOVE_METRICS_ENABLED =
    buildConf("insertOverwrite.removeMetrics.enabled")
      .internal()
      .doc(
        """
          |When enabled, insert operations in overwrite mode will add metrics describing
          |removed data to table's history""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val LOG_SIZE_IN_MEMORY_THRESHOLD =
    buildConf("streaming.logSizeInMemoryThreshold")
      .internal()
      .doc(
        """
          |The threshold of transaction log file size to read into the memory. When a file is larger
          |than this, we will read the log file in multiple passes rather than loading it into
          |the memory entirely.""".stripMargin)
      .longConf
      .createWithDefault(128L * 1024 * 1024) // 128MB

  val STREAMING_OFFSET_VALIDATION =
    buildConf("streaming.offsetValidation.enabled")
      .internal()
      .doc("Whether to validate whether delta streaming source generates a smaller offset and " +
        "moves backward.")
      .booleanConf
      .createWithDefault(true)

  val LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS =
    buildConf("loadFileSystemConfigsFromDataFrameOptions")
      .internal()
      .doc(
        """Whether to load file systems configs provided in DataFrameReader/Writer options when
          |calling `DataFrameReader.load/DataFrameWriter.save` using a Delta table path.
          |`DataFrameReader.table/DataFrameWriter.saveAsTable` doesn't support this.""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val CONVERT_EMPTY_TO_NULL_FOR_STRING_PARTITION_COL =
    buildConf("convertEmptyToNullForStringPartitionCol")
      .internal()
      .doc(
        """
          |If true, always convert empty string to null for string partition columns before
          |constraint checks.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_ENABLED =
    buildConf("skipping.partitionLikeFilters.enabled")
      .doc(
        """
           |If true, during data skipping, apply arbitrary data filters to "partition-like"
           |files (files with the same min-max values and no nulls on all referenced attributes).
           |""".stripMargin)
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_THRESHOLD =
    buildConf("skipping.partitionLikeDataSkippingFilesThreshold")
      .internal()
      .doc("Partition-like data skipping on files with the same min-max values will only be" +
        "attempted when a Delta table has a number of files larger than this threshold.")
      .intConf
      .createWithDefault(100)

  val DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_CLUSTERING_COLUMNS_ONLY =
    buildConf("skipping.partitionLikeDataSkipping.limitToClusteringColumns")
      .internal()
      .doc("Limits partition-like data skipping to filters referencing only clustering columns" +
        "In general, clustering columns will be most likely to produce files with the same" +
        "min-max values, though this restriction might exclude filters on columns highly " +
        "correlated with the clustering columns.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_ADDITIONAL_SUPPORTED_EXPRESSIONS =
    buildConf("skipping.partitionLikeDataSkipping.additionalSupportedExpressions")
      .internal()
      .doc("Comma-separated list of the canonical class names of additional expressions for which" +
        "partition-like data skipping can be safely applied.")
      .stringConf
      .createOptional

  val DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_ENABLED =
    buildConf("skipping.enhancedIsNullPushdownExprs.enabled")
      .doc("If true, support pushing down IsNull on additional null-intolerant expressions for " +
        "data skipping.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_MAX_DEPTH =
    buildConf("skipping.enhancedIsNullPushdownExprs.maxDepth")
      .doc("The maximum number of times a complex expression like Or or And would have an IsNull " +
        "pushed down in it for data skipping.")
      .internal()
      .intConf
      .createWithDefault(8)

  /**
   * The below confs have a special prefix `spark.databricks.io` because this is the conf value
   * already used by Databricks' data skipping implementation. There's no benefit to making OSS
   * users, some of whom are Databricks customers, have to keep track of two different conf
   * values for the same data skipping parameter.
   */
  val DATA_SKIPPING_STRING_PREFIX_LENGTH =
    SQLConf.buildConf("spark.databricks.io.skipping.stringPrefixLength")
      .internal()
      .doc("For string columns, how long prefix to store in the data skipping index.")
      .intConf
      .createWithDefault(32)

  val MDC_NUM_RANGE_IDS =
    SQLConf.buildConf("spark.databricks.io.skipping.mdc.rangeId.max")
      .internal()
      .doc("This controls the domain of rangeId values to be interleaved. The bigger, the better " +
         "granularity, but at the expense of performance (more data gets sampled).")
      .intConf
      .checkValue(_ > 1, "'spark.databricks.io.skipping.mdc.rangeId.max' must be greater than 1")
      .createWithDefault(1000)

  val MDC_ADD_NOISE =
    SQLConf.buildConf("spark.databricks.io.skipping.mdc.addNoise")
      .internal()
      .doc("Whether or not a random byte should be added as a suffix to the interleaved bits " +
         "when computing the Z-order values for MDC. This can help deal with skew, but may " +
         "have a negative impact on overall min/max skipping effectiveness.")
      .booleanConf
      .createWithDefault(true)

  val MDC_SORT_WITHIN_FILES =
    SQLConf.buildConf("spark.databricks.io.skipping.mdc.sortWithinFiles")
      .internal()
      .doc("If enabled, sort within files by the specified MDC curve. " +
         "This might improve row-group skipping and data compression, at " +
         "the cost of additional overhead for sorting.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_OPTIMIZE_ZORDER_COL_STAT_CHECK =
    buildConf("optimize.zorder.checkStatsCollection.enabled")
      .internal()
      .doc(s"When enabled, we will check if the column we're actually collecting stats " +
        "on the columns we are z-ordering on.")
      .booleanConf
      .createWithDefault(true)

  val FAST_INTERLEAVE_BITS_ENABLED =
    buildConf("optimize.zorder.fastInterleaveBits.enabled")
      .internal()
      .doc("When true, a faster version of the bit interleaving algorithm is used.")
      .booleanConf
      .createWithDefault(false)

  val INTERNAL_UDF_OPTIMIZATION_ENABLED =
    buildConf("internalUdfOptimization.enabled")
      .internal()
      .doc(
        """If true, create udfs used by Delta internally from templates to reduce lock contention
          |caused by Scala Reflection.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_OPTIMIZE_CONDITIONAL_INCREMENT_METRIC_ENABLED =
    buildConf("optimize.conditionalIncrementMetric.enabled")
      .internal()
      .doc("Whether to enable optimization of ConditionalIncrementMetric expressions with " +
        "constant conditions.")
      .booleanConf
      .createWithDefault(true)

  val GENERATED_COLUMN_PARTITION_FILTER_OPTIMIZATION_ENABLED =
    buildConf("generatedColumn.partitionFilterOptimization.enabled")
      .internal()
      .doc(
      "Whether to extract partition filters automatically from data filters for a partition" +
        " generated column if possible")
      .booleanConf
      .createWithDefault(true)

  val GENERATED_COLUMN_ALLOW_NULLABLE =
    buildConf("generatedColumn.allowNullableIngest.enabled")
      .internal()
      .doc("When enabled this will allow tables with generated columns enabled to be able " +
        "to write data without providing values for a nullable column via DataFrame.write")
      .booleanConf
      .createWithDefault(true)

  object GeneratedColumnValidateOnWriteMode extends Enumeration {
    val OFF, LOG_ONLY, ASSERT = Value

    def fromConf(conf: SQLConf): Value =
      withName(conf.getConf(GENERATED_COLUMN_VALIDATE_ON_WRITE))

    def default: Value =
      withName(GENERATED_COLUMN_VALIDATE_ON_WRITE.defaultValueString)
  }

  val GENERATED_COLUMN_VALIDATE_ON_WRITE =
    buildConf("generatedColumn.validateOnWrite.enabled")
      .internal()
      .doc("When enabled, validates generated column expressions during write operations to " +
        "protect against disallowed expressions.")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(GeneratedColumnValidateOnWriteMode.values.map(_.toString))
      .createWithDefault(GeneratedColumnValidateOnWriteMode.LOG_ONLY.toString)

  object ValidateCheckConstraintsMode extends Enumeration {
    val OFF, LOG_ONLY, ASSERT = Value

    def fromConf(conf: SQLConf): Value =
      withName(conf.getConf(VALIDATE_CHECK_CONSTRAINTS))

    def default: Value =
      withName(VALIDATE_CHECK_CONSTRAINTS.defaultValueString)
  }

  val VALIDATE_CHECK_CONSTRAINTS =
    buildConf("checkConstraints.validation.enabled")
      .internal()
      .doc("When enabled, validates check constraints expressions during both creation and write" +
        " paths to protect against disallowed expressions.")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(ValidateCheckConstraintsMode.values.map(_.toString))
      .createWithDefault(ValidateCheckConstraintsMode.LOG_ONLY.toString)

  val DELTA_CONVERT_ICEBERG_ENABLED =
    buildConf("convert.iceberg.enabled")
      .internal()
      .doc("If enabled, Iceberg tables can be converted into a Delta table.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_CONVERT_ICEBERG_PARTITION_EVOLUTION_ENABLED =
    buildConf("convert.iceberg.partitionEvolution.enabled")
      .doc("If enabled, support conversion of iceberg tables experienced partition evolution.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_CONVERT_ICEBERG_BUCKET_PARTITION_ENABLED =
    buildConf("convert.iceberg.bucketPartition.enabled")
      .doc("If enabled, convert iceberg table with bucket partition to unpartitioned delta table.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_CONVERT_ICEBERG_CAST_TIME_TYPE = {
    buildConf("convert.iceberg.castTimeType")
      .internal()
      .doc("Cast Iceberg TIME type to Spark Long when converting to Delta")
      .booleanConf
      .createWithDefault(false)
  }

  final object NonDeterministicPredicateWidening {
    final val OFF = "off"
    final val LOGGING = "logging"
    final val ON = "on"

    final val list = Set(OFF, LOGGING, ON)
  }

  val DELTA_CONFLICT_DETECTION_WIDEN_NONDETERMINISTIC_PREDICATES =
    buildConf("conflictDetection.partitionLevelConcurrency.widenNonDeterministicPredicates")
      .doc("Whether to widen non-deterministic predicates during partition-level concurrency. " +
        "Widening can lead to additional conflicts." +
        "When the value is 'off', non-deterministic predicates are not widened during conflict " +
        "resolution." +
        "The value 'logging' will log whether the widening of non-deterministic predicates lead " +
        "to additional conflicts. The conflict resolution is still done without widening. " +
        "When the value is 'on', non-deterministic predicates are widened during conflict " +
        "resolution.")
      .internal()
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValues(NonDeterministicPredicateWidening.list)
      .createWithDefault(NonDeterministicPredicateWidening.ON)

  val DELTA_CONFLICT_DETECTION_ALLOW_REPLACE_TABLE_TO_REMOVE_NEW_DOMAIN_METADATA =
    buildConf("conflictDetection.allowReplaceTableToRemoveNewDomainMetadata")
      .doc("Whether to allow removing new domain metadatas from concurrent transactions during " +
        "conflict resolution for a REPLACE TABLE operation. Note that this flag applies only " +
        "to metadata domains where the table snapshot read by the REPLACE TABLE command did " +
        "not contain a domain metadata of the same domain.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_UNIFORM_ICEBERG_SYNC_CONVERT_ENABLED =
    buildConf("uniform.iceberg.sync.convert.enabled")
      .doc("If enabled, iceberg conversion will be done synchronously. " +
        "This can cause slow down in Delta commits and should only be used " +
        "for debugging or in test suites.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DELTA_UNIFORM_HUDI_SYNC_CONVERT_ENABLED =
    buildConf("uniform.hudi.sync.convert.enabled")
      .doc("If enabled, Hudi conversion will be done synchronously.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DELTA_UNIFORM_ICEBERG_RETRY_TIMES =
    buildConf("uniform.iceberg.retry.times")
      .doc("The number of retries iceberg conversions should have in case " +
        "of failures")
      .internal()
      .intConf
      .createWithDefault(3)

  val DELTA_UNIFORM_ICEBERG_INCLUDE_BASE_CONVERTED_VERSION =
    buildConf("uniform.iceberg.include.base.converted.version")
      .doc("If true, include the base converted delta version as a tbl property in Iceberg " +
        "metadata to indicate the delta version that the conversion started from")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_OPTIMIZE_MIN_FILE_SIZE =
    buildConf("optimize.minFileSize")
        .internal()
        .doc(
          """Files which are smaller than this threshold (in bytes) will be grouped together
             | and rewritten as larger files by the OPTIMIZE command.
             |""".stripMargin)
        .longConf
        .checkValue(_ >= 0, "minFileSize has to be positive")
        .createWithDefault(1024 * 1024 * 1024)

  val DELTA_OPTIMIZE_MAX_FILE_SIZE =
    buildConf("optimize.maxFileSize")
        .internal()
        .doc("Target file size produced by the OPTIMIZE command.")
        .longConf
        .checkValue(_ >= 0, "maxFileSize has to be positive")
        .createWithDefault(1024 * 1024 * 1024)

  val DELTA_OPTIMIZE_MAX_THREADS =
    buildConf("optimize.maxThreads")
        .internal()
        .doc(
          """
            |Maximum number of parallel jobs allowed in OPTIMIZE command. Increasing the maximum
            | parallel jobs allows the OPTIMIZE command to run faster, but increases the job
            | management on the Spark driver side.
            |""".stripMargin)
        .intConf
        .checkValue(_ > 0, "'optimize.maxThreads' must be positive.")
        .createWithDefault(15)

  val DELTA_OPTIMIZE_BATCH_SIZE =
    buildConf("optimize.batchSize")
        .internal()
        .doc(
          """
            |The size of a batch within an OPTIMIZE JOB. After a batch is complete, its
            | progress will be committed to the transaction log, allowing for incremental
            | progress.
            |""".stripMargin)
        .bytesConf(ByteUnit.BYTE)
        .checkValue(_ > 0, "batchSize has to be positive")
        .createOptional

  val DELTA_OPTIMIZE_REPARTITION_ENABLED =
    buildConf("optimize.repartition.enabled")
      .internal()
      .doc("Use repartition(1) instead of coalesce(1) to merge small files. " +
        "coalesce(1) is executed with only one task, if there are many tiny files " +
        "within a bin (e.g. 1000 files of 50MB), it cannot be optimized with more executors. " +
        "repartition(1) incurs a shuffle stage, but the job can be distributed."
      )
      .booleanConf
      .createWithDefault(false)

  val DELTA_ALTER_TABLE_CHANGE_COLUMN_CHECK_EXPRESSIONS =
    buildConf("alterTable.changeColumn.checkExpressions")
      .internal()
      .doc(
        """
          |Given an ALTER TABLE command that changes columns, check if there are expressions used
          | in Check Constraints and Generated Columns that reference this column and thus will
          | be affected by this change.
          |
          |This is a safety switch - we should only turn this off when there is an issue with
          |expression checking logic that prevents a valid column change from going through.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_LIQUID_ALTER_COLUMN_AFTER_STATS_SCHEMA_CHECK =
    buildConf("liquid.alterColumnAfter.statsSchemaCheck")
      .internal()
      .doc(
         """
           |When enabled, validates that clustering columns remain in the stats schema after
           | a user executes `ALTER TABLE ALTER COLUMN col1 AFTER col2`. The validation checks
           | that all clustering columns that were in the stats schema before the column reordering
           | remain in the stats schema after the operation. This ensures that clustering columns
           | continue to have statistics collected even if their position in the table schema
           | changes. When disabled, no validation is performed and stats collection may follow
           | position-based indexing rules (e.g., `dataSkippingNumIndexedCols`), potentially
           | causing clustering columns to lose stats collection if they move outside the indexed
           | range.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_CHANGE_COLUMN_CHECK_DEPENDENT_EXPRESSIONS_USE_V2 =
    buildConf("changeColumn.checkDependentExpressionsUseV2")
      .internal()
      .doc(
        """
          |More accurate implementation of checker for altering/renaming/dropping columns
          |that might be referenced by constraints or generation rules.
          |It respects nested arrays and maps, unlike the V1 checker.
          |
          |This is a safety switch - we should only turn this off when there is an issue with
          |expression checking logic that prevents a valid column change from going through.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_RENAME_COLUMN_ESCAPE_NAME =
    buildConf("changeColumn.renameColumnEscapeName")
      .internal()
      .doc(
        """
          |Properly escape column names when renaming a column in the metadata.
          |
          |This is a safety switch - we should only set this to false if the fix introduces some
          |regression.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_ALTER_TABLE_DROP_COLUMN_ENABLED =
    buildConf("alterTable.dropColumn.enabled")
      .internal()
      .doc(
        """Whether to enable the drop column feature for Delta.
          |This is a safety switch - we should only turn this off when there is an issue.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_BYPASS_CHARVARCHAR_TO_STRING_FIX =
    buildConf("alterTable.bypassCharVarcharToStringFix")
      .internal()
      .doc(
        """Whether to bypass the fix for CHAR/VARCHAR to STRING type conversion in ALTER TABLE.
          |This is a safety switch - we should only set this to true if the fix introduces some
          |regression.
          |The fix in question strips CHAR/VARCHAR metadata from columns and converts
          |StringType to CHAR/VARCHAR Type temporarily during alter table column commands.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP = {
    buildConf("changeDataFeed.timestampOutOfRange.enabled")
      .doc(
        """When enabled, Change Data Feed queries with starting and ending timestamps
           | exceeding the newest delta commit timestamp will not error out. For starting timestamp
           | out of range we will return an empty DataFrame, for ending timestamps out of range we
           | will consider the latest Delta version as the ending version.""".stripMargin)
      .booleanConf
      .createWithDefault(false)
  }

  val DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_COLUMN_MAPPING_SCHEMA_CHANGES =
    buildConf("streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled")
      .doc(
        "Streaming read on Delta table with column mapping schema operations " +
          "(e.g. rename or drop column) is currently blocked due to potential data loss and " +
        "schema confusion. However, existing users may use this flag to force unblock " +
          "if they'd like to take the risk.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_SCHEMA_CHANGES_DURING_STREAM_START =
    buildConf("streaming.unsafeReadOnIncompatibleSchemaChangesDuringStreamStart.enabled")
      .doc(
        """A legacy config to disable schema read-compatibility check on the start version schema
          |when starting a streaming query. The config is added to allow legacy problematic queries
          |disabling the check to keep running if users accept the potential risks of incompatible
          |schema reading.""".stripMargin)
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DELTA_STREAMING_UNSAFE_READ_ON_PARTITION_COLUMN_CHANGE =
    buildConf("streaming.unsafeReadOnPartitionColumnChanges.enabled")
      .doc(
        "Streaming read on Delta table with partition column overwrite " +
          "(e.g. changing partition column) is currently blocked due to potential data loss. " +
          "However, existing users may use this flag to force unblock " +
          "if they'd like to take the risk.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DELTA_STREAMING_IGNORE_INTERNAL_METADATA_FOR_SCHEMA_CHANGE =
    buildConf("streaming.ignoreInternalMetadataForSchemaChange.enabled")
      .doc(
        "Whether to ignore internal metadata attached to struct fields when detecting schema " +
        "changes in Delta sources, e.g. identity columns internal high-water mark tracking.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_STREAMING_ENABLE_SCHEMA_TRACKING =
    buildConf("streaming.schemaTracking.enabled")
      .doc(
        """If enabled, Delta streaming source can support non-additive schema evolution for
          |operations such as rename or drop column on column mapping enabled tables.
          |""".stripMargin)
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_STREAMING_ENABLE_SCHEMA_TRACKING_MERGE_CONSECUTIVE_CHANGES =
    buildConf("streaming.schemaTracking.mergeConsecutiveSchemaChanges.enabled")
      .doc(
        "When enabled, schema tracking in Delta streaming would consider multiple consecutive " +
          "schema changes as one.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_STREAMING_ALLOW_SCHEMA_LOCATION_OUTSIDE_CHECKPOINT_LOCATION =
    buildConf("streaming.allowSchemaLocationOutsideCheckpointLocation")
      .doc(
        "When enabled, Delta streaming can set a schema location outside of the " +
        "query's checkpoint location. This is not recommended.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DELTA_STREAMING_SCHEMA_TRACKING_METADATA_PATH_CHECK_ENABLED =
    buildConf("streaming.schemaTracking.metadataPathCheck.enabled")
      .doc(
        "When enabled, Delta streaming with schema tracking will ensure the schema log entry " +
          "must match the source's unique checkpoint metadata location.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_STREAM_UNSAFE_READ_ON_NULLABILITY_CHANGE =
    buildConf("streaming.unsafeReadOnNullabilityChange.enabled")
      .doc(
        """A legacy config to disable unsafe nullability check. The config is added to allow legacy
          |problematic queries disabling the check to keep running if users accept the potential
          |risks of incompatible schema reading.""".stripMargin)
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DELTA_STREAMING_CREATE_DATAFRAME_DROP_NULL_COLUMNS =
    buildConf("streaming.createDataFrame.dropNullColumns")
      .internal()
      .doc("Whether to drop columns with NullType in DeltaLog.createDataFrame.")
      .booleanConf
      .createWithDefault(false)

  val DELTA_CREATE_DATAFRAME_DROP_NULL_COLUMNS =
    buildConf("createDataFrame.dropNullColumns")
      .internal()
      .doc("Whether to drop columns with NullType in DeltaLog.createDataFrame.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS =
    buildConf("streaming.sink.allowImplicitCasts")
      .internal()
      .doc(
        """Whether to accept writing data to a Delta streaming sink when the data type doesn't
          |match the type in the underlying Delta table. When true, data is cast to the expected
          |type before the write. When false, the write fails.
          |The casting behavior is governed by 'spark.sql.storeAssignmentPolicy'.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_STREAMING_SINK_IMPLICIT_CAST_FOR_TYPE_MISMATCH_ONLY =
    buildConf("streaming.sink.implicitCastForTypeMismatchOnly")
      .internal()
      .doc(
        """Controls when an implicit cast is added when writing data to a Delta table using
          |streaming.
          |When true, a cast is added only when there is a type mismatch between a column or
          |nested field in the data and table schema.
          |When false, missing, extra or reordered columns or nested fields also trigger adding an
          |implicit cast.
          |Only takes effect when implicit casting is enabled in streaming writes to a Delta table
          |via `spark.databricks.delta.streaming.sink.allowImplicitCasts`.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_STREAMING_SINK_IMPLICIT_CAST_ESCAPE_COLUMN_NAMES =
    buildConf("streaming.sink.implicitCastEscapeColumnNames")
      .internal()
      .doc(
        """
          |When true, the code paths handling implicit casting in streaming will escape column names
          |to properly handle e.g. dots in column names.
          |This is a kill-switch and shouldn't be disabled unless necessary to mitigate an issue.
          |Only takes effect when implicit casting is enabled in streaming writes to a Delta table
          |via `spark.databricks.delta.streaming.sink.allowImplicitCasts`.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_CDF_UNSAFE_BATCH_READ_ON_INCOMPATIBLE_SCHEMA_CHANGES =
    buildConf("changeDataFeed.unsafeBatchReadOnIncompatibleSchemaChanges.enabled")
      .doc(
        "Reading change data in batch (e.g. using `table_changes()`) on Delta table with " +
          "column mapping schema operations is currently blocked due to potential data loss and " +
          "schema confusion. However, existing users may use this flag to force unblock " +
          "if they'd like to take the risk.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DELTA_CDF_DEFAULT_SCHEMA_MODE_FOR_COLUMN_MAPPING_TABLE =
    buildConf("changeDataFeed.defaultSchemaModeForColumnMappingTable")
      .doc(
        """Reading batch CDF on column mapping enabled table requires schema mode to be set to
           |`endVersion` so the ending version's schema will be used.
           |Set this to `latest` to use the schema of the latest available table version,
           |or to `legacy` to fallback to the non column-mapping default behavior, in which
           |the time travel option can be used to select the version of the schema.""".stripMargin)
      .internal()
      .stringConf
      .createWithDefault("endVersion")

  val DELTA_CDF_ALLOW_TIME_TRAVEL_OPTIONS =
    buildConf("changeDataFeed.allowTimeTravelOptionsForSchema")
      .doc(
        s"""If allowed, user can specify time-travel reader options such as
           |'versionAsOf' or 'timestampAsOf' to specify the read schema while
           |reading change data feed.""".stripMargin)
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DELTA_COLUMN_MAPPING_CHECK_MAX_COLUMN_ID =
    buildConf("columnMapping.checkMaxColumnId")
      .doc(
        s"""If enabled, check if delta.columnMapping.maxColumnId is correctly assigned at each
           |Delta transaction commit.
           |""".stripMargin)
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_COLUMN_MAPPING_STRIP_METADATA =
    buildConf("columnMapping.stripMetadata")
      .doc(
        """
          |Transactions might try to update the schema of a table with columns that contain
          |column mapping metadata, even when column mapping is not enabled. For example, this
          |can happen when transactions copy the schema from another table. When this setting is
          |enabled, we will strip the column mapping metadata from the schema before applying it.
          |Note that this config applies only when the existing schema of the table does not
          |contain any column mapping metadata.
          |""".stripMargin)
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_COLUMN_MAPPING_DISALLOW_ENABLING_WHEN_METADATA_ALREADY_EXISTS =
    buildConf("columnMapping.disallowEnablingWhenColumnMappingMetadataAlreadyExists")
      .doc(
        """
          |If Delta table already has column mapping metadata before the feature is enabled, it is
          |as a result of a corruption or a bug. Enabling column mapping in such a case can lead to
          |further corruption of the table and should be disallowed.
          |""".stripMargin)
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DYNAMIC_PARTITION_OVERWRITE_ENABLED =
    buildConf("dynamicPartitionOverwrite.enabled")
      .doc("Whether to overwrite partitions dynamically when 'partitionOverwriteMode' is set to " +
        "'dynamic' in either the SQL conf, or a DataFrameWriter option. When this is disabled " +
        "'partitionOverwriteMode' will be ignored.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val ALLOW_ARBITRARY_TABLE_PROPERTIES =
    buildConf("allowArbitraryProperties.enabled")
      .doc(
      """Whether we allow arbitrary Delta table properties. When this is enabled, table properties
          |with the prefix 'delta.' are not checked for validity. Table property validity is based
          |on the current Delta version being used and feature support in that version. Arbitrary
          |properties without the 'delta.' prefix are always allowed regardless of this config.
          |
          |Please use with caution. When enabled, there will be no warning when unsupported table
          |properties for the Delta version being used are set, or when properties are set
          |incorrectly (for example, misspelled).""".stripMargin
      )
      .internal()
      .booleanConf
      .createWithDefault(false)

  val TABLE_BUILDER_FORCE_TABLEPROPERTY_LOWERCASE =
    buildConf("deltaTableBuilder.forceTablePropertyLowerCase.enabled")
      .internal()
      .doc(
        """Whether the keys of table properties should be set to lower case.
          | Turn on this flag if you want keys of table properties not starting with delta
          | to be backward compatible when the table is created via DeltaTableBuilder
          | Please note that if you set this to true, the lower case of the
          | key will be used for non delta prefix table properties.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val DELTA_REQUIRED_SPARK_CONFS_CHECK =
    buildConf("requiredSparkConfsCheck.enabled")
      .doc("Whether to verify SparkSession is initialized with required configurations.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val RESTORE_TABLE_PROTOCOL_DOWNGRADE_ALLOWED =
    buildConf("restore.protocolDowngradeAllowed")
      .doc("""
        | Whether a table RESTORE or CLONE operation may downgrade the protocol of the table.
        | Note that depending on the protocol and the enabled table features, downgrading the
        | protocol may break snapshot reconstruction and make the table unreadable. Protocol
        | downgrades may also make the history unreadable.""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val DELTA_CLONE_REPLACE_ENABLED =
    buildConf("clone.replaceEnabled")
      .internal()
      .doc("If enabled, the table will be replaced when cloning over an existing Delta table.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_OPTIMIZE_METADATA_QUERY_ENABLED =
    buildConf("optimizeMetadataQuery.enabled")
      .internal()
      .doc("Whether we can use the metadata in the DeltaLog to" +
        " optimize queries that can be run purely on metadata.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_SKIP_RECORDING_EMPTY_COMMITS =
    buildConf("skipRecordingEmptyCommits")
      .internal()
      .doc(
        """
          | Whether to skip recording an empty commit in the Delta Log. This only works when table
          | is using SnapshotIsolation or Serializable Isolation Mode.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val REPLACE_TABLE_PROTOCOL_DOWNGRADE_ALLOWED =
  buildConf("replace.protocolDowngradeAllowed")
    .internal()
    .doc("""
       | Whether a REPLACE operation may downgrade the protocol of the table.
       | Note that depending on the protocol and the enabled table features, downgrading the
       | protocol may break snapshot reconstruction and make the table unreadable. Protocol
       | downgrades may also make the history unreadable.""".stripMargin)
    .booleanConf
    .createWithDefault(false)

  //////////////////
  // Idempotent DML
  //////////////////

  val DELTA_IDEMPOTENT_DML_TXN_APP_ID =
    buildConf("write.txnAppId")
      .internal()
      .doc("""
             |The application ID under which this write will be committed.
             | If specified, spark.databricks.delta.write.txnVersion also needs to
             | be set.
             |""".stripMargin)
      .stringConf
      .createOptional

  val DELTA_IDEMPOTENT_DML_TXN_VERSION =
    buildConf("write.txnVersion")
      .internal()
      .doc("""
             |The user-defined version under which this write will be committed.
             | If specified, spark.databricks.delta.write.txnAppId also needs to
             | be set. To ensure idempotency, txnVersions across different writes
             | need to be monotonically increasing.
             |""".stripMargin)
      .longConf
      .createOptional

  val DELTA_IDEMPOTENT_DML_AUTO_RESET_ENABLED =
    buildConf("write.txnVersion.autoReset.enabled")
      .internal()
      .doc("""
             |If true, will automatically reset spark.databricks.delta.write.txnVersion
             |after every write. This is false by default.
             |""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val DELTA_OPTIMIZE_MAX_DELETED_ROWS_RATIO =
    buildConf("optimize.maxDeletedRowsRatio")
      .internal()
      .doc("Files with a ratio of deleted rows to the total rows larger than this threshold " +
        "will be rewritten by the OPTIMIZE command.")
      .doubleConf
      .checkValue(_ >= 0, "maxDeletedRowsRatio must be in range [0.0, 1.0]")
      .checkValue(_ <= 1, "maxDeletedRowsRatio must be in range [0.0, 1.0]")
      .createWithDefault(0.05d)

  val DELTA_TABLE_PROPERTY_CONSTRAINTS_CHECK_ENABLED =
    buildConf("tablePropertyConstraintsCheck.enabled")
      .internal()
      .doc(
        """Check that all table-properties satisfy validity constraints.
          |Only change this for testing!""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_DUPLICATE_ACTION_CHECK_ENABLED =
    buildConf("duplicateActionCheck.enabled")
      .internal()
      .doc("""
             |Verify only one action is specified for each file path in one commit.
             |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELETE_USE_PERSISTENT_DELETION_VECTORS =
    buildConf("delete.deletionVectors.persistent")
      .internal()
      .doc("Enable persistent Deletion Vectors in the Delete command.")
      .booleanConf
      .createWithDefault(true)

  val MERGE_USE_PERSISTENT_DELETION_VECTORS =
    buildConf("merge.deletionVectors.persistent")
      .internal()
      .doc("Enable persistent Deletion Vectors in Merge command.")
      .booleanConf
      .createWithDefault(true)

  val UPDATE_USE_PERSISTENT_DELETION_VECTORS =
    buildConf("update.deletionVectors.persistent")
      .internal()
      .doc("Enable persistent Deletion Vectors in the Update command.")
      .booleanConf
      .createWithDefault(true)

  val DELETION_VECTOR_PACKING_TARGET_SIZE =
    buildConf("deletionVectors.packing.targetSize")
      .internal()
      .doc("Controls the target file deletion vector file size when packing multiple" +
        "deletion vectors in a single file.")
      .bytesConf(ByteUnit.BYTE)
      /**
       * A [[DeletionVectorDescriptor]] stores an offset as a 32-bit integer into the file where the
       * deletion vector is stored. There is a hard limit of ~2.1GB for this file before the offset
       * integer overflows. Since we do bin packing with estimates, we set a lower internal
       * limit to be safe.
       */
      .checkValue(_ >= 0, "deletionVectors.packing.targetSize must be non-negative")
      .checkValue(_ < 3L * 1024L * 1024L * 1024L / 2L,
         "deletionVectors.packing.targetSize must be less than 1.5GB")
      .createWithDefault(2L * 1024L * 1024L)

  val TIGHT_BOUND_COLUMN_ON_FILE_INIT_DISABLED =
    buildConf("deletionVectors.disableTightBoundOnFileCreationForDevOnly")
      .internal()
      .doc("""Controls whether we generate a tightBounds column in statistics on file creation.
             |The tightBounds column annotates whether the statistics of the file are tight or wide.
             |This flag is only used for testing purposes.
                """.stripMargin)
      .booleanConf
      .createWithDefault(false)

  val DELETION_VECTORS_USE_METADATA_ROW_INDEX =
    buildConf("deletionVectors.useMetadataRowIndex")
      .internal()
      .doc(
        """Controls whether we use the Parquet reader generated row_index column for
          | filtering deleted rows with deletion vectors. When enabled, it allows
          | predicate pushdown and file splitting in scans.""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val WRITE_DATA_FILES_TO_SUBDIR = buildConf("write.dataFilesToSubdir")
    .internal()
    .doc("Delta will write all data files to subdir 'data/' under table dir if enabled")
    .booleanConf
    .createWithDefault(false)

  val DELETION_VECTORS_COMMIT_CHECK_ENABLED =
    buildConf("deletionVectors.skipCommitCheck")
      .internal()
      .doc(
        """Check the table-property and verify that deletion vectors may be added
          |to this table.
          |Only change this for testing!""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val REUSE_COLUMN_MAPPING_METADATA_DURING_OVERWRITE =
    buildConf("columnMapping.reuseColumnMetadataDuringOverwrite")
      .internal()
      .doc(
        """
          |If enabled, when a column mapping table is overwritten, the new schema will reuse as many
          |old schema's column mapping metadata (field id and physical name) as possible.
          |This allows the analyzed schema from prior to the overwrite to be still read-compatible
          |with the data post the overwrite, enabling better user experience when, for example,
          |the column mapping table is being continuously scanned in a streaming query, the analyzed
          |table schema will still be readable after the table is overwritten.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val REUSE_COLUMN_METADATA_DURING_REPLACE_TABLE =
    buildConf("columnMapping.reuseColumnMetadataDuringReplace")
      .internal()
      .doc(
        """
          |If enabled, when a column mapping table is replaced, the new schema will reuse as many
          |old schema's column mapping metadata (field id and physical name) as possible.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val ALLOW_COLUMN_MAPPING_REMOVAL =
    buildConf("columnMapping.allowRemoval")
      .internal()
      .doc(
        """
          |If enabled, allow the column mapping to be removed from a table.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTALOG_MINOR_COMPACTION_USE_FOR_READS =
    buildConf("deltaLog.minorCompaction.useForReads")
      .doc("If true, minor compacted delta log files will be used for creating Snapshots")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val ICEBERG_MAX_COMMITS_TO_CONVERT = buildConf("iceberg.maxPendingCommits")
    .doc("""
        |The maximum number of pending Delta commits to convert to Iceberg incrementally.
        |If the table hasn't been converted to Iceberg in longer than this number of commits,
        |we start from scratch, replacing the previously converted Iceberg table contents.
        |""".stripMargin)
    .intConf
    .createWithDefault(100)

  val HUDI_MAX_COMMITS_TO_CONVERT = buildConf("hudi.maxPendingCommits")
    .doc("""
           |The maximum number of pending Delta commits to convert to Hudi incrementally.
           |If the table hasn't been converted to Hudi in longer than this number of commits,
           |we start from scratch, replacing the previously converted Hudi table contents.
           |""".stripMargin)
    .intConf
    .createWithDefault(100)

  val ICEBERG_MAX_ACTIONS_TO_CONVERT = buildConf("iceberg.maxPendingActions")
    .doc("""
        |[Deprecated]
        |The maximum number of pending Delta actions to convert to Iceberg incrementally.
        |If there are more than this number of outstanding actions, chunk them into separate
        |Iceberg commits.
        |""".stripMargin)
    .intConf
    .createWithDefault(100 * 1000)

  val UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG =
    buildConf("updateAndMergeCastingFollowsAnsiEnabledFlag")
      .internal()
      .doc("""If false, casting behaviour in implicit casts in UPDATE and MERGE follows
             |'spark.sql.storeAssignmentPolicy'. If true, these casts follow 'ansi.enabled'.
             |""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val DELTA_USE_MULTI_THREADED_STATS_COLLECTION =
    buildConf("collectStats.useMultiThreadedStatsCollection")
      .internal()
      .doc("Whether to use multi-threaded statistics collection. If false, statistics will be " +
        "collected sequentially within each partition.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_STATS_COLLECTION_NUM_FILES_PARTITION =
    buildConf("collectStats.numFilesPerPartition")
      .internal()
      .doc("Controls the number of files that should be within a RDD partition " +
        "during multi-threaded optimized statistics collection. A larger number will lead to " +
        "less parallelism, but can reduce scheduling overhead.")
      .intConf
      .checkValue(v => v >= 1, "Must be at least 1.")
      .createWithDefault(100)

  val DELTA_STATS_COLLECTION_FALLBACK_TO_INTERPRETED_PROJECTION =
    buildConf("collectStats.fallbackToInterpretedProjection")
      .internal()
      .doc("When enabled, the updateStats expression will use the standard code path" +
        " that falls back to an interpreted expression if codegen fails. This should" +
        " always be true. The config only exists to force the old behavior, which was" +
        " to always use codegen.")
      .booleanConf
      .createWithDefault(true)

  val DELTA_CONVERT_ICEBERG_STATS = buildConf("collectStats.convertIceberg")
    .internal()
    .doc("When enabled, attempts to convert Iceberg stats to Delta stats when cloning from " +
      "an Iceberg source.")
    .booleanConf
    .createWithDefault(true)

  val DELTA_CONVERT_ICEBERG_DECIMAL_STATS = buildConf("collectStats.convertIceberg.decimal")
    .internal()
    .doc("When enabled, attempts to convert Iceberg stats for DECIMAL to Delta stats" +
      "when cloning from an Iceberg source.")
    .booleanConf
    .createWithDefault(true)

  val DELTA_CONVERT_ICEBERG_DATE_STATS = buildConf("collectStats.convertIceberg.date")
    .internal()
    .doc("When enabled, attempts to convert Iceberg stats for DATE to Delta stats" +
      "when cloning from an Iceberg source.")
    .booleanConf
    .createWithDefault(true)

  val DELTA_CONVERT_ICEBERG_TIMESTAMP_STATS = buildConf("collectStats.convertIceberg.timestamp")
    .internal()
    .doc("When enabled, attempts to convert Iceberg stats for TIMESTAMP to Delta stats" +
      "when cloning from an Iceberg source.")
    .booleanConf
    .createWithDefault(true)

  /**
   * For iceberg clone,
   * When stats conversion from iceberg off, fallback to slow stats conversion enabled
   * When stats conversion from iceberg on,
   *  fallback to slow stats conversion will not happen if partial stats conversion enabled
   *  fallback only happens if partial stats conversion disabled and iceberg has partial stats
   *  - either minValues or maxValues is missing
   */
  val DELTA_CLONE_ICEBERG_ALLOW_PARTIAL_STATS =
    buildConf("clone.iceberg.allowPartialStats")
      .internal()
      .doc("If true, allow converting partial stats from iceberg stats " +
        "to delta stats during clone."
      )
      .booleanConf
      .createWithDefault(true)

  /////////////////////
  // Optimized Write
  /////////////////////

  val DELTA_OPTIMIZE_WRITE_ENABLED =
    buildConf("optimizeWrite.enabled")
      .doc("Whether to optimize writes made into Delta tables from this session.")
      .booleanConf
      .createOptional

  val DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS =
    buildConf("optimizeWrite.numShuffleBlocks")
      .internal()
      .doc("Maximum number of shuffle blocks to target for the adaptive shuffle " +
        "in optimized writes.")
      .intConf
      .createWithDefault(50000000)

  val SKIP_REDIRECT_FEATURE =
    buildConf("skipRedirectFeature")
      .doc("True if skipping the redirect feature.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val ENABLE_TABLE_REDIRECT_FEATURE =
    buildConf("enableTableRedirectFeature")
      .doc("True if enabling the table redirect feature.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS =
    buildConf("optimizeWrite.maxShufflePartitions")
      .internal()
      .doc("Max number of output buckets (reducers) that can be used by optimized writes. This " +
        "can be thought of as: 'how many target partitions are we going to write to in our " +
        "table in one write'. This should not be larger than " +
        "spark.shuffle.minNumPartitionsToHighlyCompress. Otherwise, partition coalescing and " +
        "skew split may not work due to incomplete stats from HighlyCompressedMapStatus")
      .intConf
      .createWithDefault(2000)

  val DELTA_OPTIMIZE_WRITE_BIN_SIZE =
    buildConf("optimizeWrite.binSize")
      .internal()
      .doc("Bin size for the adaptive shuffle in optimized writes in megabytes.")
      .bytesConf(ByteUnit.MiB)
      .createWithDefault(512)

  val DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE =
  buildConf("optimize.clustering.mergeStrategy.minCubeSize.threshold")
    .internal()
    .doc(
      "Z-cube size at which new data will no longer be merged with it during incremental " +
        "OPTIMIZE."
    )
    .longConf
    .checkValue(_ >= 0, "the threshold must be >= 0")
    .createWithDefault(100 * DELTA_OPTIMIZE_MAX_FILE_SIZE.defaultValue.get)

  val DELTA_OPTIMIZE_CLUSTERING_TARGET_CUBE_SIZE =
  buildConf("optimize.clustering.mergeStrategy.minCubeSize.targetCubeSize")
    .internal()
    .doc(
      "Target size of the Z-cubes we will create. This is not a hard max; we will continue " +
        "adding files to a Z-cube until their combined size exceeds this value. This value " +
        s"must be greater than or equal to ${DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE.key}. "
    )
    .longConf
    .checkValue(_ >= 0, "the target must be >= 0")
    .createWithDefault((DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE.defaultValue.get * 1.5).toLong)

  //////////////////
  // Clustered Table
  //////////////////

  val DELTA_NUM_CLUSTERING_COLUMNS_LIMIT =
    buildStaticConf("clusteredTable.numClusteringColumnsLimit")
      .internal()
      .doc("""The maximum number of clustering columns allowed for a clustered table.
        """.stripMargin)
      .intConf
      .checkValue(
        _ > 0,
        "'clusteredTable.numClusteringColumnsLimit' must be positive."
      )
    .createWithDefault(4)

  val DELTA_LOG_CACHE_SIZE = buildConf("delta.log.cacheSize")
    .internal()
    .doc("The maximum number of DeltaLog instances to cache in memory.")
    .longConf
    .createWithDefault(10000)

  val DELTA_LOG_CACHE_RETENTION_MINUTES = buildConf("delta.log.cacheRetentionMinutes")
    .internal()
    .doc("The rentention duration of DeltaLog instances in the cache")
    .timeConf(TimeUnit.MINUTES)
    .createWithDefault(60)

  //////////////////
  // Delta Sharing
  //////////////////

  val DELTA_SHARING_ENABLE_DELTA_FORMAT_BATCH =
    buildConf("spark.sql.delta.sharing.enableDeltaFormatBatch")
      .doc("Enable delta format sharing in case of issues.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val DELTA_SHARING_FORCE_DELTA_FORMAT =
    buildConf("spark.sql.delta.sharing.forceDeltaFormat")
      .doc("Force queries to use delta format when no responseFormat is specified.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  ///////////////////
  // IDENTITY COLUMN
  ///////////////////

  val DELTA_IDENTITY_COLUMN_ENABLED =
    buildConf("identityColumn.enabled")
      .internal()
      .doc(
        """
          | The umbrella config to turn on/off the IDENTITY column support.
          | If true, enable Delta IDENTITY column write support. If a table has an IDENTITY column,
          | it is not writable but still readable if this config is set to false.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

  val DELTA_IDENTITY_ALLOW_SYNC_IDENTITY_TO_LOWER_HIGH_WATER_MARK =
    buildConf("identityColumn.allowSyncIdentityToLowerHighWaterMark.enabled")
      .internal()
      .doc(
        """
          | If true, the SYNC IDENTITY command can reduce the high water mark in a Delta IDENTITY
          | column. If false, the high water mark will only be updated if it
          | respects the column's specified start, step, and existing high watermark value.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  ///////////
  // VARIANT
  ///////////////////
  val FORCE_USE_PREVIEW_VARIANT_FEATURE = buildConf("variant.forceUsePreviewTableFeature")
    .internal()
    .doc(
      """
        | If true, creating new tables with variant columns only attaches the 'variantType-preview'
        | table feature. Attempting to operate on existing tables created with the stable feature
        | does not require that the preview table feature be present.
        |""".stripMargin)
    .booleanConf
    .createWithDefault(false)

  val FORCE_USE_PREVIEW_SHREDDING_FEATURE =
    buildConf("variantShredding.forceUsePreviewTableFeature")
    .internal()
    .doc(
      """
        | If true, attach the 'variantShredding-preview' table feature when enabling shredding
        | on a table. When false, the 'variantShredding' feature is used instead.""".stripMargin)
    .booleanConf
    .createWithDefault(true)

  val COLLECT_VARIANT_DATA_SKIPPING_STATS =
    buildConf("variantShredding.collectVariantDataSkippingStats")
    .internal()
    .doc(
      """
        | If enabled, Spark writes to Delta could collect data skipping stats for Variant
        | columns. Currently, this config is used to ensure that new checkpoints preserve previous
        | Variant stats."""
        .stripMargin)
    .booleanConf
    .createWithDefault(true)

  ///////////
  // TESTING
  ///////////
  val DELTA_POST_COMMIT_HOOK_THROW_ON_ERROR =
    buildConf("postCommitHook.throwOnError")
      .internal()
      .doc("If true, post-commit hooks will by default throw an exception when they fail.")
      .booleanConf
      .createWithDefault(DeltaUtils.isTesting)

  val TEST_FILE_NAME_PREFIX =
    buildStaticConf("testOnly.dataFileNamePrefix")
      .internal()
      .doc("[TEST_ONLY]: The prefix to use for the names of all Parquet data files.")
      .stringConf
      .createWithDefault(if (DeltaUtils.isTesting) "test%file%prefix-" else "")

  val TEST_DV_NAME_PREFIX =
    buildStaticConf("testOnly.dvFileNamePrefix")
      .internal()
      .doc("[TEST_ONLY]: The prefix to use for the names of all Deletion Vector files.")
      .stringConf
      .createWithDefault(if (DeltaUtils.isTesting) "test%dv%prefix-" else "")

  ///////////
  // UTC TIMESTAMP PARTITION VALUES
  ///////////////////
  val UTC_TIMESTAMP_PARTITION_VALUES = buildConf("write.utcTimestampPartitionValues")
    .internal()
    .doc(
      """
        | If true, write UTC normalized timestamp partition values to Delta Log.
        |""".stripMargin)
    .booleanConf
    .createWithDefault(true)

  /////////////////////////////////////
  // NORMALIZE PARTITION VALUES ON READ
  ////////////////////////////////////

  val DELTA_NORMALIZE_PARTITION_VALUES_ON_READ =
    buildConf("normalizePartitionValuesOnRead")
      .internal()
      .doc(
        "When true, we will normalize partition values on read by parsing them " +
        "to their actual types for comparison instead of using raw strings. This helps prevent " +
        "issues with inconsistently formatted partition values. " +
        "UTC_TIMESTAMP_PARTITION_VALUES normalized timestamp partition values on write. However, " +
        "data written before this flag existed may not be normalized and needs to be normalized " +
        "on read."
      )
      .booleanConf
      .createWithDefault(true)

  //////////////////
  // CORRECTNESS
  //////////////////

  val NUM_RECORDS_VALIDATION_ENABLED =
    buildConf("numRecordsValidation.enabled")
      .internal()
      .doc(
        """
          |When enabled, adds a check to MERGE, UPDATE and DELETE that validates the number of
          |records that were added and removed.
          |
          |- For MERGE without INSERT statements it checks that the number of records does not
          |  increase.
          |- For MERGE without DELETE statements it checks that the number of records does not
          |  decrease.
          |- For UPDATE statements it checks that the number of records does not change.
          |- For DELETE statements it checks that the number of records does not increase.
          |
          |When disabled, we only log a warning.
          |""".stripMargin
      )
      .booleanConf
      .createWithDefault(true)


  val COMMAND_INVARIANT_CHECKS_USE_UNRELIABLE =
    buildConf("commandInvariantChecksUseUnreliable")
      .internal()
      .doc("When enabled all DML commands will check and log invariants using unreliable metrics.")
      .booleanConf
      .createWithDefault(true)

  val COMMAND_INVARIANT_CHECKS_THROW =
    buildConf("commandInvariantChecksThrow")
      .internal()
      .doc(
        """When disabled all DML commands using reliable metrics just log a warning on command
          |invariant violation and proceed to commit.
          |When enabled, it's decided by a per-command flag.""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val ENABLE_SERVER_SIDE_PLANNING =
    buildConf("catalog.enableServerSidePlanning")
      .internal()
      .doc(
        """When enabled, DeltaCatalog will use server-side scan planning path
          |instead of normal table loading.""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  /**
   * Controls which connector implementation to use for Delta table operations.
   *
   * Valid values:
   * - NONE: sparkV2 connector is disabled, always use sparkV1 connector (DeltaTableV2) - default
   * - AUTO: Automatically use sparkV2 connector (SparkTable) for Unity Catalog managed tables
   *         in streaming queries and sparkV1 connector (DeltaTableV2) for all other tables
   * - STRICT: sparkV2 connector is strictly enforced, always use sparkV2 connector (SparkTable).
   *           Intended for testing sparkV2 connector capabilities
   *
   * sparkV1 vs sparkV2 Connectors:
   * - sparkV1 Connector (DeltaTableV2): Legacy Delta connector with full read/write support,
   *   uses DeltaLog for metadata management
   * - sparkV2 Connector (SparkTable): New kernel-based connector with read-only support,
   *   uses Kernel's Table API for metadata management
   *
   * See [[org.apache.spark.sql.delta.DeltaV2Mode]] for the centralized logic that interprets
   * this configuration.
   */
  val V2_ENABLE_MODE =
    buildConf("v2.enableMode")
      .doc(
        "Controls the Delta connector enable mode. " +
          "NONE (use v1 connector for all cases), AUTO (use v2 only for v2 " +
          "supported operations, default), STRICT (should ONLY be enabled for testing).")
      .stringConf
      .checkValues(Set("AUTO", "NONE", "STRICT"))
      .createWithDefault("AUTO")

  val DELTA_STREAMING_INITIAL_SNAPSHOT_MAX_FILES =
    buildConf("streaming.initialSnapshotMaxFiles")
      .internal()
      .doc("Maximum number of files allowed in initial snapshot for V2 streaming.")
      .intConf
      .createWithDefault(50000)
}

object DeltaSQLConf extends DeltaSQLConfBase
