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

package org.apache.spark.sql.delta

import java.util.Locale
import java.util.regex.PatternSyntaxException

import scala.util.Try
import scala.util.matching.Regex

import org.apache.spark.sql.delta.DeltaOptions.{DATA_CHANGE_OPTION, MERGE_SCHEMA_OPTION, OVERWRITE_SCHEMA_OPTION}
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

  val failOnDataLoss = options.get(FAIL_ON_DATA_LOSS_OPTION)
    .forall(toBoolean(_, FAIL_ON_DATA_LOSS_OPTION)) // thanks to forall: by default true


  val excludeRegex: Option[Regex] = try options.get(EXCLUDE_REGEX_OPTION).map(_.r) catch {
    case e: PatternSyntaxException =>
      throw new IllegalArgumentException(
        s"Please recheck your syntax for '$EXCLUDE_REGEX_OPTION'", e)
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

  provideOneStartingOption()
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

  val MAX_FILES_PER_TRIGGER_OPTION = "maxFilesPerTrigger"
  val MAX_FILES_PER_TRIGGER_OPTION_DEFAULT = 1000
  val MAX_BYTES_PER_TRIGGER_OPTION = "maxBytesPerTrigger"
  val EXCLUDE_REGEX_OPTION = "excludeRegex"
  val IGNORE_FILE_DELETION_OPTION = "ignoreFileDeletion"
  val IGNORE_CHANGES_OPTION = "ignoreChanges"
  val IGNORE_DELETES_OPTION = "ignoreDeletes"
  val FAIL_ON_DATA_LOSS_OPTION = "failOnDataLoss"
  val OPTIMIZE_WRITE_OPTION = "optimizeWrite"
  val DATA_CHANGE_OPTION = "dataChange"
  val STARTING_VERSION_OPTION = "startingVersion"
  val STARTING_TIMESTAMP_OPTION = "startingTimestamp"

  val validOptionKeys : Set[String] = Set(
    REPLACE_WHERE_OPTION,
    MERGE_SCHEMA_OPTION,
    EXCLUDE_REGEX_OPTION,
    OVERWRITE_SCHEMA_OPTION,
    USER_METADATA_OPTION,
    MAX_FILES_PER_TRIGGER_OPTION,
    IGNORE_FILE_DELETION_OPTION,
    IGNORE_CHANGES_OPTION,
    IGNORE_DELETES_OPTION,
    FAIL_ON_DATA_LOSS_OPTION,
    OPTIMIZE_WRITE_OPTION,
    DATA_CHANGE_OPTION,
    STARTING_TIMESTAMP_OPTION,
    STARTING_VERSION_OPTION,
    "queryName",
    "checkpointLocation",
    "path",
    "timestampAsOf",
    "versionAsOf"
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
 * Definitions for the starting version of a Delta stream.
 */
sealed trait DeltaStartingVersion
case object StartingVersionLatest extends DeltaStartingVersion
case class StartingVersion(version: Long) extends DeltaStartingVersion
