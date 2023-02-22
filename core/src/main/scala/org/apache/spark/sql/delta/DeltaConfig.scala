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

package org.apache.spark.sql.delta

import java.util.{HashMap, Locale}

import org.apache.spark.sql.delta.actions.{Action, Metadata, Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DataSkippingReader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.{DateTimeConstants, IntervalUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.Utils

case class DeltaConfig[T](
    key: String,
    defaultValue: String,
    fromString: String => T,
    validationFunction: T => Boolean,
    helpMessage: String,
    minimumProtocolVersion: Option[Protocol] = None,
    editable: Boolean = true,
    alternateKeys: Seq[String] = Seq.empty) {
  /**
   * Recover the saved value of this configuration from `Metadata`. If undefined, fall back to
   * alternate keys, returning defaultValue if none match.
   */
  def fromMetaData(metadata: Metadata): T = {
    for (usedKey <- key +: alternateKeys) {
      metadata.configuration.get(usedKey).map { value => return fromString(value) }
    }
    fromString(defaultValue)
  }

  /** Validate the setting for this configuration */
  private def validate(value: String): Unit = {
    if (!editable) {
      throw DeltaErrors.cannotModifyTableProperty(key)
    }
    val onErrorMessage = s"$key $helpMessage"
    try {
      require(validationFunction(fromString(value)), onErrorMessage)
    } catch {
      case e: NumberFormatException =>
        throw new IllegalArgumentException(onErrorMessage, e)
    }
  }

  /**
   * Validate this configuration and return the key - value pair to save into the metadata.
   */
  def apply(value: String): (String, String) = {
    validate(value)
    key -> value
  }

  /**
   * SQL configuration to set for ensuring that all newly created tables have this table property.
   */
  def defaultTablePropertyKey: String = DeltaConfigs.sqlConfPrefix + key.stripPrefix("delta.")
}

/**
 * Contains list of reservoir configs and validation checks.
 */
trait DeltaConfigsBase extends DeltaLogging {

  /**
   * Convert a string to [[CalendarInterval]]. This method is case-insensitive and will throw
   * [[IllegalArgumentException]] when the input string is not a valid interval.
   *
   * TODO Remove this method and use `CalendarInterval.fromCaseInsensitiveString` instead when
   * upgrading Spark. This is a fork version of `CalendarInterval.fromCaseInsensitiveString` which
   * will be available in the next Spark release (See SPARK-27735).
   *
   * @throws IllegalArgumentException if the string is not a valid internal.
   */
  def parseCalendarInterval(s: String): CalendarInterval = {
    if (s == null || s.trim.isEmpty) {
      throw DeltaErrors.emptyCalendarInterval
    }
    val sInLowerCase = s.trim.toLowerCase(Locale.ROOT)
    val interval =
      if (sInLowerCase.startsWith("interval ")) sInLowerCase else "interval " + sInLowerCase
    val cal = IntervalUtils.safeStringToInterval(UTF8String.fromString(interval))
    if (cal == null) {
      throw DeltaErrors.invalidInterval(s)
    }
    cal
  }

  /**
   * A global default value set as a SQLConf will overwrite the default value of a DeltaConfig.
   * For example, user can run:
   *   set spark.delta.properties.defaults.randomPrefixLength = 5
   * This setting will be populated to a Delta table during its creation time and overwrites
   * the default value of delta.randomPrefixLength.
   *
   * We accept these SQLConfs as strings and only perform validation in DeltaConfig. All the
   * DeltaConfigs set in SQLConf should adopt the same prefix.
   */
  val sqlConfPrefix = "spark.delta.properties.defaults."
  val deprecatedSqlConfPrefix = "spark.databricks.delta.properties.defaults."

  private val entries = new HashMap[String, DeltaConfig[_]]

  protected def buildConfig[T](
      key: String,
      defaultValue: String,
      fromString: String => T,
      validationFunction: T => Boolean,
      helpMessage: String,
      minimumProtocolVersion: Option[Protocol] = None,
      userConfigurable: Boolean = true,
      alternateConfs: Seq[DeltaConfig[T]] = Seq.empty): DeltaConfig[T] = {

    val deltaConfig = DeltaConfig(s"delta.$key",
      defaultValue,
      fromString,
      validationFunction,
      helpMessage,
      minimumProtocolVersion,
      userConfigurable,
      alternateConfs.map(_.key))

    entries.put(key.toLowerCase(Locale.ROOT), deltaConfig)
    deltaConfig
  }

  /**
   * Validates specified configurations and returns the normalized key -> value map.
   */
  def validateConfigurations(configurations: Map[String, String]): Map[String, String] = {
    val allowArbitraryProperties = SparkSession.active.sessionState.conf
      .getConf(DeltaSQLConf.ALLOW_ARBITRARY_TABLE_PROPERTIES)

    configurations.map { case kv @ (key, value) =>
      key.toLowerCase(Locale.ROOT) match {
        case lKey if lKey.startsWith("delta.constraints.") =>
          // This is a CHECK constraint, we should allow it.
          kv
        case lKey if lKey.startsWith(TableFeatureProtocolUtils.FEATURE_PROP_PREFIX) =>
          // This is a table feature, we should allow it.
          lKey -> value
        case lKey if lKey.startsWith("delta.") =>
          Option(entries.get(lKey.stripPrefix("delta."))) match {
            case Some(deltaConfig) => deltaConfig(value) // validate the value
            case None if allowArbitraryProperties =>
              logConsole(
                s"You are setting a property: $key that is not recognized by this " +
                  "version of Delta")
              kv
            case None => throw DeltaErrors.unknownConfigurationKeyException(key)
          }
        case _ =>
          if (entries.containsKey(key)) {
            logConsole(s"""
              |You are trying to set a property the key of which is the same as Delta config: $key.
              |If you are trying to set a Delta config, prefix it with "delta.", e.g. 'delta.$key'.
            """.stripMargin)
          }
          kv
      }
    }
  }

  /**
   * Table properties for new tables can be specified through SQL Configurations using the
   * [[sqlConfPrefix]] (though, we must also support [[deprecatedSqlConfPrefix]].
   *
   * This method checks to see if any of the configurations exist among the SQL configurations and
   * merges them with the user provided configurations. User provided configs (`tableConf`) take
   * precedence.
   */
  def mergeGlobalConfigs(sqlConfs: SQLConf, tableConf: Map[String, String]): Map[String, String] = {
    import collection.JavaConverters._

    val globalConfs = entries.asScala.flatMap { case (key, config) =>
      val sqlConfKey = sqlConfPrefix + config.key.stripPrefix("delta.")
      val deprecatedSqlConfKey = deprecatedSqlConfPrefix + config.key.stripPrefix("delta.")

      val sqlConf = Option(sqlConfs.getConfString(sqlConfKey, null))
      val deprecatedSqlConf = Option(sqlConfs.getConfString(deprecatedSqlConfKey, null))

      (sqlConf, deprecatedSqlConf) match {
        case (None, None) => None
        case (Some(default), None) => Some(config(default))
        case (None, Some(default)) => Some(config(default))
        case (Some(sqlConfVal), Some(deprecatedSqlConfVal)) =>
          if (sqlConfVal == deprecatedSqlConfVal) {
            Some(config(sqlConfVal))
          } else {
            throw DeltaErrors.ambiguousConfigurationKeysException(sqlConfKey, sqlConfVal,
              deprecatedSqlConfKey, deprecatedSqlConfVal)
          }
      }
    }

    globalConfs.toMap ++ tableConf
  }

  /**
   * Normalize the specified property keys if the key is for a Delta config.
   */
  def normalizeConfigKeys(propKeys: Seq[String]): Seq[String] = {
    propKeys.map { key =>
      key.toLowerCase(Locale.ROOT) match {
        case lKey if lKey.startsWith(TableFeatureProtocolUtils.FEATURE_PROP_PREFIX) =>
          lKey
        case lKey if lKey.startsWith("delta.") =>
          Option(entries.get(lKey.stripPrefix("delta."))).map(_.key).getOrElse(key)
        case _ => key
      }
    }
  }

  /**
   * Normalize the specified property key if the key is for a Delta config.
   */
  def normalizeConfigKey(propKey: Option[String]): Option[String] = {
    propKey.map { key =>
      key.toLowerCase(Locale.ROOT) match {
        case lKey if lKey.startsWith(TableFeatureProtocolUtils.FEATURE_PROP_PREFIX) =>
          lKey
        case lKey if lKey.startsWith("delta.") =>
          Option(entries.get(lKey.stripPrefix("delta."))).map(_.key).getOrElse(key)
        case _ => key
      }
    }
  }

  def getMilliSeconds(i: CalendarInterval): Long = {
    getMicroSeconds(i) / 1000L
  }

  private def getMicroSeconds(i: CalendarInterval): Long = {
    assert(i.months == 0)
    i.days * DateTimeConstants.MICROS_PER_DAY + i.microseconds
  }

  /**
   * For configs accepting an interval, we require the user specified string must obey:
   *
   * - Doesn't use months or years, since an internal like this is not deterministic.
   * - The microseconds parsed from the string value must be a non-negative value.
   *
   * The method returns whether a [[CalendarInterval]] satisfies the requirements.
   */
  def isValidIntervalConfigValue(i: CalendarInterval): Boolean = {
    i.months == 0 && getMicroSeconds(i) >= 0
  }

  /**
   * The protocol reader version modelled as a table property. This property is *not* stored as
   * a table property in the `Metadata` action. It is stored as its own action. Having it modelled
   * as a table property makes it easier to upgrade, and view the version.
   */
  val MIN_READER_VERSION = buildConfig[Int](
    "minReaderVersion",
    Action.supportedProtocolVersion().minReaderVersion.toString,
    _.toInt,
    v => v > 0 && v <= Action.supportedProtocolVersion().minReaderVersion,
    s"needs to be an integer between [1, ${Action.supportedProtocolVersion().minReaderVersion}].")

  /**
   * The protocol reader version modelled as a table property. This property is *not* stored as
   * a table property in the `Metadata` action. It is stored as its own action. Having it modelled
   * as a table property makes it easier to upgrade, and view the version.
   */
  val MIN_WRITER_VERSION = buildConfig[Int](
    "minWriterVersion",
    Action.supportedProtocolVersion().minWriterVersion.toString,
    _.toInt,
    v => v > 0 && v <= Action.supportedProtocolVersion().minWriterVersion,
    s"needs to be an integer between [1, ${Action.supportedProtocolVersion().minWriterVersion}].")

  /**
   * The shortest duration we have to keep delta files around before deleting them. We can only
   * delete delta files that are before a compaction. We may keep files beyond this duration until
   * the next calendar day.
   */
  val LOG_RETENTION = buildConfig[CalendarInterval](
    "logRetentionDuration",
    "interval 30 days",
    parseCalendarInterval,
    isValidIntervalConfigValue,
    "needs to be provided as a calendar interval such as '2 weeks'. Months " +
    "and years are not accepted. You may specify '365 days' for a year instead.")

  /**
   * The shortest duration we have to keep delta sample files around before deleting them.
   */
  val SAMPLE_RETENTION = buildConfig[CalendarInterval](
    "sampleRetentionDuration",
    "interval 7 days",
    parseCalendarInterval,
    isValidIntervalConfigValue,
    "needs to be provided as a calendar interval such as '2 weeks'. Months " +
      "and years are not accepted. You may specify '365 days' for a year instead.")

  /**
   * The shortest duration we have to keep checkpoint files around before deleting them. Note that
   * we'll never delete the most recent checkpoint. We may keep checkpoint files beyond this
   * duration until the next calendar day.
   */
  val CHECKPOINT_RETENTION_DURATION = buildConfig[CalendarInterval](
    "checkpointRetentionDuration",
    "interval 2 days",
    parseCalendarInterval,
    isValidIntervalConfigValue,
    "needs to be provided as a calendar interval such as '2 weeks'. Months " +
      "and years are not accepted. You may specify '365 days' for a year instead.")

  /** How often to checkpoint the delta log. */
  val CHECKPOINT_INTERVAL = buildConfig[Int](
    "checkpointInterval",
    "10",
    _.toInt,
    _ > 0,
    "needs to be a positive integer.")

  /** Whether to clean up expired checkpoints and delta logs. */
  val ENABLE_EXPIRED_LOG_CLEANUP = buildConfig[Boolean](
    "enableExpiredLogCleanup",
    "true",
    _.toBoolean,
    _ => true,
    "needs to be a boolean.")

  /**
   * If true, a delta table can be rolled back to any point within LOG_RETENTION. Leaving this on
   * requires converting the oldest delta file we have into a checkpoint, which we do once a day. If
   * doing that operation is too expensive, it can be turned off, but the table can only be rolled
   * back CHECKPOINT_RETENTION_DURATION ago instead of LOG_RETENTION ago.
   */
  val ENABLE_FULL_RETENTION_ROLLBACK = buildConfig[Boolean](
    "enableFullRetentionRollback",
    "true",
    _.toBoolean,
    _ => true,
    "needs to be a boolean."
  )

  /**
   * The shortest duration we have to keep logically deleted data files around before deleting them
   * physically. This is to prevent failures in stale readers after compactions or partition
   * overwrites.
   *
   * Note: this value should be large enough:
   * - It should be larger than the longest possible duration of a job if you decide to run "VACUUM"
   *   when there are concurrent readers or writers accessing the table.
   * - If you are running a streaming query reading from the table, you should make sure the query
   *   doesn't stop longer than this value. Otherwise, the query may not be able to restart as it
   *   still needs to read old files.
   */
  val TOMBSTONE_RETENTION = buildConfig[CalendarInterval](
    "deletedFileRetentionDuration",
    "interval 1 week",
    parseCalendarInterval,
    isValidIntervalConfigValue,
    "needs to be provided as a calendar interval such as '2 weeks'. Months " +
    "and years are not accepted. You may specify '365 days' for a year instead.")

  /**
   * Whether to use a random prefix in a file path instead of partition information. This is
   * required for very high volume S3 calls to better be partitioned across S3 servers.
   */
  val RANDOMIZE_FILE_PREFIXES = buildConfig[Boolean](
    "randomizeFilePrefixes",
    "false",
    _.toBoolean,
    _ => true,
    "needs to be a boolean.")

  /**
   * Whether to use a random prefix in a file path instead of partition information. This is
   * required for very high volume S3 calls to better be partitioned across S3 servers.
   */
  val RANDOM_PREFIX_LENGTH = buildConfig[Int](
    "randomPrefixLength",
    "2",
    _.toInt,
    a => a > 0,
    "needs to be greater than 0.")

  /**
   * Whether this Delta table is append-only. Files can't be deleted, or values can't be updated.
   */
  val IS_APPEND_ONLY = buildConfig[Boolean](
    key = "appendOnly",
    defaultValue = "false",
    fromString = _.toBoolean,
    validationFunction = _ => true,
    helpMessage = "needs to be a boolean.",
    minimumProtocolVersion = Some(AppendOnlyTableFeature.minProtocolVersion))

  /**
   * Whether commands modifying this Delta table are allowed to create new deletion vectors.
   */
  val ENABLE_DELETION_VECTORS_CREATION = buildConfig[Boolean](
    key = "enableDeletionVectors",
    defaultValue = "false",
    fromString = _.toBoolean,
    validationFunction = _ => true,
    helpMessage = "needs to be a boolean.",
    minimumProtocolVersion = Some(DeletionVectorsTableFeature.minProtocolVersion))

  /**
   * Whether this table will automatically optimize the layout of files during writes.
   */
  val AUTO_OPTIMIZE = buildConfig[Option[Boolean]](
    "autoOptimize",
    null,
    v => Option(v).map(_.toBoolean),
    _ => true,
    "needs to be a boolean.")

  /**
   * The number of columns to collect stats on for data skipping. A value of -1 means collecting
   * stats for all columns. Updating this conf does not trigger stats re-collection, but redefines
   * the stats schema of table, i.e., it will change the behavior of future stats collection
   * (e.g., in append and OPTIMIZE) as well as data skipping (e.g., the column stats beyond this
   * number will be ignored even when they exist).
   */
  val DATA_SKIPPING_NUM_INDEXED_COLS = buildConfig[Int](
    "dataSkippingNumIndexedCols",
    DataSkippingReader.DATA_SKIPPING_NUM_INDEXED_COLS_DEFAULT_VALUE.toString,
    _.toInt,
    a => a >= -1,
    "needs to be larger than or equal to -1.")

  val SYMLINK_FORMAT_MANIFEST_ENABLED = buildConfig[Boolean](
    s"${hooks.GenerateSymlinkManifest.CONFIG_NAME_ROOT}.enabled",
    "false",
    _.toBoolean,
    _ => true,
    "needs to be a boolean.")

  /**
   * When enabled, we will write file statistics in the checkpoint in JSON format as the "stats"
   * column.
   */
  val CHECKPOINT_WRITE_STATS_AS_JSON = buildConfig[Boolean](
    "checkpoint.writeStatsAsJson",
    "true",
    _.toBoolean,
    _ => true,
    "needs to be a boolean.")

  /**
   * When enabled, we will write file statistics in the checkpoint in the struct format in the
   * "stats_parsed" column. We will also write partition values as a struct as
   * "partitionValues_parsed".
   */
  val CHECKPOINT_WRITE_STATS_AS_STRUCT = buildConfig[Option[Boolean]](
    "checkpoint.writeStatsAsStruct",
    null,
    v => Option(v).map(_.toBoolean),
    _ => true,
    "needs to be a boolean.")

  /**
   * Deprecated in favor of CHANGE_DATA_FEED.
   */
  private val CHANGE_DATA_FEED_LEGACY = buildConfig[Boolean](
    "enableChangeDataCapture",
    "false",
    _.toBoolean,
    _ => true,
    "needs to be a boolean.")

  /**
   * Enable change data feed output.
   * When enabled, DELETE, UPDATE, and MERGE INTO operations will need to do additional work to
   * output their change data in an efficiently readable format.
   */
  val CHANGE_DATA_FEED = buildConfig[Boolean](
    "enableChangeDataFeed",
    "false",
    _.toBoolean,
    _ => true,
    "needs to be a boolean.",
    alternateConfs = Seq(CHANGE_DATA_FEED_LEGACY),
    minimumProtocolVersion = Some(ChangeDataFeedTableFeature.minProtocolVersion))

  val COLUMN_MAPPING_MODE = buildConfig[DeltaColumnMappingMode](
    "columnMapping.mode",
    "none",
    DeltaColumnMappingMode(_),
    _ => true,
    "",
    minimumProtocolVersion = Some(ColumnMappingTableFeature.minProtocolVersion))

  /**
   * Maximum columnId used in the schema so far for column mapping. Internal property that cannot
   * be set by users.
   */
  val COLUMN_MAPPING_MAX_ID = buildConfig[Long](
    "columnMapping.maxColumnId",
    "0",
    _.toLong,
    _ => true,
    "",
    minimumProtocolVersion = Some(ColumnMappingTableFeature.minProtocolVersion),
    userConfigurable = false)


  /**
   * The shortest duration within which new [[Snapshot]]s will retain transaction identifiers (i.e.
   * [[SetTransaction]]s). When a new [[Snapshot]] sees a transaction identifier older than or equal
   * to the specified TRANSACTION_ID_RETENTION_DURATION, it considers it expired and ignores it.
   */
  val TRANSACTION_ID_RETENTION_DURATION = buildConfig[Option[CalendarInterval]](
    "setTransactionRetentionDuration",
    null,
    v => if (v == null) None else Some(parseCalendarInterval(v)),
    opt => opt.forall(isValidIntervalConfigValue),
    "needs to be provided as a calendar interval such as '2 weeks'. Months " +
      "and years are not accepted. You may specify '365 days' for a year instead.")

  /**
   * The isolation level of a table defines the degree to which a transaction must be isolated from
   * modifications made by concurrent transactions. Delta currently supports one isolation level:
   * Serializable.
   */
  val ISOLATION_LEVEL = buildConfig[IsolationLevel](
    "isolationLevel",
    Serializable.toString,
    IsolationLevel.fromString(_),
    _ == Serializable,
    "must be Serializable"
  )
}

object DeltaConfigs extends DeltaConfigsBase
