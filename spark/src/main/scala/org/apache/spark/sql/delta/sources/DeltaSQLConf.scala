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
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

/**
 * [[SQLConf]] entries for Delta features.
 */
trait DeltaSQLConfBase {
  val SQL_CONF_PREFIX = "spark.databricks.delta"

  def buildConf(key: String): ConfigBuilder = SQLConf.buildConf(s"$SQL_CONF_PREFIX.$key")
  def buildStaticConf(key: String): ConfigBuilder =
    SQLConf.buildStaticConf(s"spark.databricks.delta.$key")

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

  val DELTA_PROTOCOL_DEFAULT_WRITER_VERSION =
    buildConf("properties.defaults.minWriterVersion")
      .doc("The default writer protocol version to create new tables with, unless a feature " +
        "that requires a higher version for correctness is enabled.")
      .intConf
      .checkValues(Set(1, 2, 3, 4, 5, 7))
      .createWithDefault(2)

  val DELTA_PROTOCOL_DEFAULT_READER_VERSION =
    buildConf("properties.defaults.minReaderVersion")
      .doc("The default reader protocol version to create new tables with, unless a feature " +
        "that requires a higher version for correctness is enabled.")
      .intConf
      .checkValues(Set(1, 2, 3))
      .createWithDefault(1)

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

  val DELTA_VACUUM_LOGGING_ENABLED =
    buildConf("vacuum.logging.enabled")
      .doc("Whether to log vacuum information into the Delta transaction log." +
        " Users should only set this config to 'true' when the underlying file system safely" +
        " supports concurrent writes.")
      .booleanConf
      .createOptional

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

  val DELTA_SCHEMA_AUTO_MIGRATE =
    buildConf("schema.autoMerge.enabled")
      .doc("If true, enables schema merging on appends and on overwrites.")
      .booleanConf
      .createWithDefault(false)

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
      .doc("When to materializes source plan during MERGE execution. " +
        "The value 'none' means source will never be materialized. " +
        "The value 'all' means source will always be materialized. " +
        "The value 'auto' means sources will not be materialized when they are certain to be " +
        "deterministic."
      )
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValues(MergeMaterializeSource.list)
      .createWithDefault(MergeMaterializeSource.AUTO)

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

  val MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL_RETRY =
    buildConf("merge.materializeSource.rddStorageLevelRetry")
      .internal()
      .doc("What StorageLevel to use to persist the source RDD when MERGE is retried. " +
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
      .createWithDefault("DISK_ONLY_2")

  val MERGE_MATERIALIZE_SOURCE_MAX_ATTEMPTS =
    buildStaticConf("merge.materializeSource.maxAttempts")
      .doc("How many times to try MERGE with in case of lost RDD materialized source data")
      .intConf
      .createWithDefault(4)

  val MERGE_MATERIALIZE_SOURCE_EAGER =
    buildConf("merge.materializeSource.eager")
      .internal()
      .doc("Materialize the source eagerly before Job 1")
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

  ////////////////////////////////////
  // Checkpoint V2 Specific Configs
  ////////////////////////////////////

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

  val DELTA_WRITE_CHECKSUM_ENABLED =
    buildConf("writeChecksumFile.enabled")
      .doc("Whether the checksum file can be written.")
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

  val DELTA_CONVERT_ICEBERG_UNSAFE_MOR_TABLE_ENABLE =
    buildConf("convert.iceberg.unsafeConvertMorTable.enabled")
      .doc("If enabled, iceberg merge-on-read tables can be unsafely converted by ignoring " +
        "deletion files. This could cause data duplication and is strongly not recommended.")
      .internal()
      .booleanConf
      .createWithDefault(false)

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

  val DELTA_ALTER_TABLE_DROP_COLUMN_ENABLED =
    buildConf("alterTable.dropColumn.enabled")
      .internal()
      .doc(
        """Whether to enable the drop column feature for Delta.
          |This is a safety switch - we should only turn this off when there is an issue.
          |""".stripMargin)
      .booleanConf
      .createWithDefault(true)

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

  val DELETION_VECTOR_PACKING_TARGET_SIZE =
    buildConf("deletionVectors.packing.targetSize")
      .internal()
      .doc("Controls the target file deletion vector file size when packing multiple" +
        "deletion vectors in a single file.")
      .bytesConf(ByteUnit.BYTE)
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

  val TABLE_FEATURE_DROP_ENABLED =
    buildConf("tableFeatures.dropEnabled")
      .internal()
      .doc("""Controls whether table feature removal is allowed.
             |Table feature removal is currently a feature in development.
             |This is a dev only config.""".stripMargin)
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

  val ICEBERG_MAX_COMMITS_TO_CONVERT = buildConf("iceberg.maxPendingCommits")
    .doc("""
        |The maximum number of pending Delta commits to convert to Iceberg incrementally.
        |If the table hasn't been converted to Iceberg in longer than this number of commits,
        |we start from scratch, replacing the previously converted Iceberg table contents.
        |""".stripMargin)
    .intConf
    .createWithDefault(100)

  val ICEBERG_MAX_ACTIONS_TO_CONVERT = buildConf("iceberg.maxPendingActions")
    .doc("""
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

}

object DeltaSQLConf extends DeltaSQLConfBase
