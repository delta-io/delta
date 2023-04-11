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

// scalastyle:off import.ordering.noEmptyLine
import java.util.Locale
import java.util.regex.PatternSyntaxException

import scala.util.Try
import scala.util.matching.Regex

import org.apache.spark.sql.delta.DeltaOptions.{DATA_CHANGE_OPTION, MERGE_SCHEMA_OPTION, OVERWRITE_SCHEMA_OPTION, PARTITION_OVERWRITE_MODE_OPTION}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf


trait DeltaOptionParser {
  protected def sqlConf: SQLConf
  protected def options: CaseInsensitiveMap[String]

  def toBoolean(input: String, name: String): Boolean = {
    Try(input.toBoolean).toOption.getOrElse {
      throw DeltaErrors.illegalDeltaOptionException(name, input, "must be 'true' or 'false'")
    }
  }
}

trait DeltaWriteOptions
  extends DeltaWriteOptionsImpl
  with DeltaOptionParser {

  import DeltaOptions._

  val replaceWhere: Option[String] = options.get(REPLACE_WHERE_OPTION)
  val userMetadata: Option[String] = options.get(USER_METADATA_OPTION)

  /**
   * Whether to add an adaptive shuffle before writing out the files to break skew, and coalesce
   * data into chunkier files.
   */
  val optimizeWrite: Option[Boolean] = options.get(OPTIMIZE_WRITE_OPTION)
    .map(toBoolean(_, OPTIMIZE_WRITE_OPTION))

}

trait DeltaWriteOptionsImpl extends DeltaOptionParser {
  import DeltaOptions._

  /**
   * Whether the user has enabled auto schema merging in writes using either a DataFrame option
   * or SQL Session configuration. Automerging is off when table ACLs are enabled.
   * We always respect the DataFrame writer configuration over the session config.
   */
  def canMergeSchema: Boolean = {
    options.get(MERGE_SCHEMA_OPTION)
      .map(toBoolean(_, MERGE_SCHEMA_OPTION))
      .getOrElse(sqlConf.getConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE))
  }

  /**
   * Whether to allow overwriting the schema of a Delta table in an overwrite mode operation. If
   * ACLs are enabled, we can't change the schema of an operation through a write, which requires
   * MODIFY permissions, when schema changes require OWN permissions.
   */
  def canOverwriteSchema: Boolean = {
    options.get(OVERWRITE_SCHEMA_OPTION).exists(toBoolean(_, OVERWRITE_SCHEMA_OPTION))
  }

  /**
   * Whether to write new data to the table or just rearrange data that is already
   * part of the table. This option declares that the data being written by this job
   * does not change any data in the table and merely rearranges existing data.
   * This makes sure streaming queries reading from this table will not see any new changes
   */
  def rearrangeOnly: Boolean = {
    options.get(DATA_CHANGE_OPTION).exists(!toBoolean(_, DATA_CHANGE_OPTION))
  }

  val txnVersion = options.get(TXN_VERSION).map { str =>
    Try(str.toLong).toOption.filter(_ >= 0).getOrElse {
      throw DeltaErrors.illegalDeltaOptionException(
        TXN_VERSION, str, "must be a non-negative integer")
    }
  }

  val txnAppId = options.get(TXN_APP_ID)

  private def validateIdempotentWriteOptions(): Unit = {
    // Either both txnVersion and txnAppId must be specified to get idempotent writes or
    // neither must be given. In all other cases, throw an exception.
    val numOptions = txnVersion.size + txnAppId.size
    if (numOptions != 0 && numOptions != 2) {
      throw DeltaErrors.invalidIdempotentWritesOptionsException("Both txnVersion and txnAppId " +
      "must be specified for idempotent data frame writes")
    }
  }

  validateIdempotentWriteOptions()

  /** Whether partitionOverwriteMode is provided as a DataFrameWriter option. */
  val partitionOverwriteModeInOptions: Boolean =
    options.contains(PARTITION_OVERWRITE_MODE_OPTION)

  /** Whether to only overwrite partitions that have data written into it at runtime. */
  def isDynamicPartitionOverwriteMode: Boolean = {
    if (!sqlConf.getConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED)) {
      // If dynamic partition overwrite mode is disabled, fallback to the default behavior
      false
    } else {
      val mode = options.get(PARTITION_OVERWRITE_MODE_OPTION)
        .getOrElse(sqlConf.getConf(SQLConf.PARTITION_OVERWRITE_MODE))
      if (!DeltaOptions.PARTITION_OVERWRITE_MODE_VALUES.exists(mode.equalsIgnoreCase(_))) {
        val acceptableStr =
          DeltaOptions.PARTITION_OVERWRITE_MODE_VALUES.map("'" + _ + "'").mkString(" or ")
        throw DeltaErrors.illegalDeltaOptionException(
          PARTITION_OVERWRITE_MODE_OPTION, mode, s"must be ${acceptableStr}"
        )
      }
      mode.equalsIgnoreCase(PARTITION_OVERWRITE_MODE_DYNAMIC)
    }
  }
}

trait DeltaReadOptions extends DeltaOptionParser {
  import DeltaOptions._

  val maxFilesPerTrigger = options.get(MAX_FILES_PER_TRIGGER_OPTION).map { str =>
    Try(str.toInt).toOption.filter(_ > 0).getOrElse {
      throw DeltaErrors.illegalDeltaOptionException(
        MAX_FILES_PER_TRIGGER_OPTION, str, "must be a positive integer")
    }
  }

  val maxBytesPerTrigger = options.get(MAX_BYTES_PER_TRIGGER_OPTION).map { str =>
    Try(JavaUtils.byteStringAs(str, ByteUnit.BYTE)).toOption.filter(_ > 0).getOrElse {
      throw DeltaErrors.illegalDeltaOptionException(
        MAX_BYTES_PER_TRIGGER_OPTION, str, "must be a size configuration such as '10g'")
    }
  }

  val ignoreFileDeletion = options.get(IGNORE_FILE_DELETION_OPTION)
    .exists(toBoolean(_, IGNORE_FILE_DELETION_OPTION))

  val ignoreChanges = options.get(IGNORE_CHANGES_OPTION).exists(toBoolean(_, IGNORE_CHANGES_OPTION))

  val ignoreDeletes = options.get(IGNORE_DELETES_OPTION).exists(toBoolean(_, IGNORE_DELETES_OPTION))

  val skipChangeCommits = options.get(SKIP_CHANGE_COMMITS_OPTION)
    .exists(toBoolean(_, SKIP_CHANGE_COMMITS_OPTION))

  val failOnDataLoss = options.get(FAIL_ON_DATA_LOSS_OPTION)
    .forall(toBoolean(_, FAIL_ON_DATA_LOSS_OPTION)) // thanks to forall: by default true

  val readChangeFeed = options.get(CDC_READ_OPTION).exists(toBoolean(_, CDC_READ_OPTION)) ||
    options.get(CDC_READ_OPTION_LEGACY).exists(toBoolean(_, CDC_READ_OPTION_LEGACY))


  val excludeRegex: Option[Regex] = try options.get(EXCLUDE_REGEX_OPTION).map(_.r) catch {
    case e: PatternSyntaxException =>
      throw DeltaErrors.excludeRegexOptionException(EXCLUDE_REGEX_OPTION, e)
  }

  val startingVersion: Option[DeltaStartingVersion] = options.get(STARTING_VERSION_OPTION).map {
    case "latest" => StartingVersionLatest
    case str =>
      Try(str.toLong).toOption.filter(_ >= 0).map(StartingVersion).getOrElse{
        throw DeltaErrors.illegalDeltaOptionException(
          STARTING_VERSION_OPTION, str, "must be greater than or equal to zero")
      }
  }

  val startingTimestamp = options.get(STARTING_TIMESTAMP_OPTION)

  private def provideOneStartingOption(): Unit = {
    if (startingTimestamp.isDefined && startingVersion.isDefined) {
      throw DeltaErrors.startingVersionAndTimestampBothSetException(
        STARTING_VERSION_OPTION,
        STARTING_TIMESTAMP_OPTION)
    }
  }

  def containsStartingVersionOrTimestamp: Boolean = {
    options.contains(STARTING_VERSION_OPTION) || options.contains(STARTING_TIMESTAMP_OPTION)
  }

  provideOneStartingOption()

  val schemaTrackingLocation = options.get(SCHEMA_TRACKING_LOCATION)

  val sourceTrackingId = options.get(STREAMING_SOURCE_TRACKING_ID)
}


/**
 * Options for the Delta data source.
 */
class DeltaOptions(
    @transient protected[delta] val options: CaseInsensitiveMap[String],
    @transient protected val sqlConf: SQLConf)
  extends DeltaWriteOptions with DeltaReadOptions with Serializable {

  DeltaOptions.verifyOptions(options)

  def this(options: Map[String, String], conf: SQLConf) = this(CaseInsensitiveMap(options), conf)
}

object DeltaOptions extends DeltaLogging {

  /** An option to overwrite only the data that matches predicates over partition columns. */
  val REPLACE_WHERE_OPTION = "replaceWhere"
  /** An option to allow automatic schema merging during a write operation. */
  val MERGE_SCHEMA_OPTION = "mergeSchema"
  /** An option to allow overwriting schema and partitioning during an overwrite write operation. */
  val OVERWRITE_SCHEMA_OPTION = "overwriteSchema"
  /** An option to specify user-defined metadata in commitInfo */
  val USER_METADATA_OPTION = "userMetadata"

  val PARTITION_OVERWRITE_MODE_OPTION = "partitionOverwriteMode"
  val PARTITION_OVERWRITE_MODE_DYNAMIC = "DYNAMIC"
  val PARTITION_OVERWRITE_MODE_STATIC = "STATIC"
  val PARTITION_OVERWRITE_MODE_VALUES =
    Set(PARTITION_OVERWRITE_MODE_STATIC, PARTITION_OVERWRITE_MODE_DYNAMIC)

  val MAX_FILES_PER_TRIGGER_OPTION = "maxFilesPerTrigger"
  val MAX_FILES_PER_TRIGGER_OPTION_DEFAULT = 1000
  val MAX_BYTES_PER_TRIGGER_OPTION = "maxBytesPerTrigger"
  val EXCLUDE_REGEX_OPTION = "excludeRegex"
  val IGNORE_FILE_DELETION_OPTION = "ignoreFileDeletion"
  val IGNORE_CHANGES_OPTION = "ignoreChanges"
  val IGNORE_DELETES_OPTION = "ignoreDeletes"
  val SKIP_CHANGE_COMMITS_OPTION = "skipChangeCommits"
  val FAIL_ON_DATA_LOSS_OPTION = "failOnDataLoss"
  val OPTIMIZE_WRITE_OPTION = "optimizeWrite"
  val DATA_CHANGE_OPTION = "dataChange"
  val STARTING_VERSION_OPTION = "startingVersion"
  val STARTING_TIMESTAMP_OPTION = "startingTimestamp"
  val CDC_START_VERSION = "startingVersion"
  val CDC_START_TIMESTAMP = "startingTimestamp"
  val CDC_END_VERSION = "endingVersion"
  val CDC_END_TIMESTAMP = "endingTimestamp"
  val CDC_READ_OPTION = "readChangeFeed"
  val CDC_READ_OPTION_LEGACY = "readChangeData"

  val VERSION_AS_OF = "versionAsOf"
  val TIMESTAMP_AS_OF = "timestampAsOf"

  val COMPRESSION = "compression"
  val MAX_RECORDS_PER_FILE = "maxRecordsPerFile"
  val TXN_APP_ID = "txnAppId"
  val TXN_VERSION = "txnVersion"

  /**
   * An option to allow column mapping enabled tables to conduct schema evolution during streaming
   */
  val SCHEMA_TRACKING_LOCATION = "schemaTrackingLocation"
  /**
   * Alias for `schemaTrackingLocation`, so users familiar with AutoLoader can migrate easily.
   */
  val SCHEMA_TRACKING_LOCATION_ALIAS = "schemaLocation"
  /**
   * An option to instruct DeltaSource to pick a customized subdirectory for schema log in case of
   * rare conflicts such as when a stream needs to do a self-union of two Delta sources from the
   * same table.
   * The final schema log location will be $parent/_schema_log_${tahoeId}_${sourceTrackingId}.
   */
  val STREAMING_SOURCE_TRACKING_ID = "streamingSourceTrackingId"


  val validOptionKeys : Set[String] = Set(
    REPLACE_WHERE_OPTION,
    MERGE_SCHEMA_OPTION,
    EXCLUDE_REGEX_OPTION,
    OVERWRITE_SCHEMA_OPTION,
    USER_METADATA_OPTION,
    PARTITION_OVERWRITE_MODE_OPTION,
    MAX_FILES_PER_TRIGGER_OPTION,
    IGNORE_FILE_DELETION_OPTION,
    IGNORE_CHANGES_OPTION,
    IGNORE_DELETES_OPTION,
    FAIL_ON_DATA_LOSS_OPTION,
    OPTIMIZE_WRITE_OPTION,
    DATA_CHANGE_OPTION,
    STARTING_TIMESTAMP_OPTION,
    STARTING_VERSION_OPTION,
    CDC_READ_OPTION,
    CDC_READ_OPTION_LEGACY,
    CDC_START_TIMESTAMP,
    CDC_END_TIMESTAMP,
    CDC_START_VERSION,
    CDC_END_VERSION,
    COMPRESSION,
    MAX_RECORDS_PER_FILE,
    TXN_APP_ID,
    TXN_VERSION,
    SCHEMA_TRACKING_LOCATION,
    SCHEMA_TRACKING_LOCATION_ALIAS,
    STREAMING_SOURCE_TRACKING_ID,
    "queryName",
    "checkpointLocation",
    "path",
    VERSION_AS_OF,
    TIMESTAMP_AS_OF
  )


  /** Iterates over all user passed options and logs any that are not valid. */
  def verifyOptions(options: CaseInsensitiveMap[String]): Unit = {
    val invalidUserOptions = SQLConf.get.redactOptions(options --
      validOptionKeys.map(_.toLowerCase(Locale.ROOT)))
    if (invalidUserOptions.nonEmpty) {
      recordDeltaEvent(null,
        "delta.option.invalid",
        data = invalidUserOptions
      )
    }
  }
}

/**
 * Definitions for the batch read schema mode for CDF
 */
sealed trait DeltaBatchCDFSchemaMode {
  def name: String
}

/**
 * `latest` batch CDF schema mode specifies that the latest schema should be used when serving
 * the CDF batch.
 */
case object BatchCDFSchemaLatest extends DeltaBatchCDFSchemaMode {
  val name = "latest"
}

/**
 * `endVersion` batch CDF schema mode specifies that the query range's end version's schema should
 * be used for serving the CDF batch.
 * This is the current default for column mapping enabled tables so we could read using the exact
 * schema at the versions being queried to reduce schema read compatibility mismatches.
 */
case object BatchCDFSchemaEndVersion extends DeltaBatchCDFSchemaMode {
  val name = "endversion"
}

/**
 * `legacy` batch CDF schema mode specifies that neither latest nor end version's schema is
 * strictly used for serving the CDF batch, e.g. when user uses TimeTravel with batch CDF and wants
 * to respect the time travelled schema.
 * This is the current default for non-column mapping tables.
 */
case object BatchCDFSchemaLegacy extends DeltaBatchCDFSchemaMode {
  val name = "legacy"
}

object DeltaBatchCDFSchemaMode {
  def apply(name: String): DeltaBatchCDFSchemaMode = {
    name.toLowerCase(Locale.ROOT) match {
      case BatchCDFSchemaLatest.name => BatchCDFSchemaLatest
      case BatchCDFSchemaEndVersion.name => BatchCDFSchemaEndVersion
      case BatchCDFSchemaLegacy.name => BatchCDFSchemaLegacy
    }
  }
}

/**
 * Definitions for the starting version of a Delta stream.
 */
sealed trait DeltaStartingVersion
case object StartingVersionLatest extends DeltaStartingVersion
case class StartingVersion(version: Long) extends DeltaStartingVersion
