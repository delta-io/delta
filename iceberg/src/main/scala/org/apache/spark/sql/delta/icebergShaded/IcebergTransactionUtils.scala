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

package org.apache.spark.sql.delta.icebergShaded

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaConfig, DeltaConfigs, DeltaErrors, DeltaLog, DeltaRuntimeException}
import org.apache.spark.sql.delta.DeltaConfigs.parseCalendarInterval
import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import shadedForDelta.org.apache.iceberg.{DataFile, DataFiles, FileFormat, PartitionSpec, Schema => IcebergSchema}
import shadedForDelta.org.apache.iceberg.Metrics
import shadedForDelta.org.apache.iceberg.TableProperties

// scalastyle:off import.ordering.noEmptyLine
import shadedForDelta.org.apache.iceberg.catalog.{Namespace, TableIdentifier => IcebergTableIdentifier}
// scalastyle:on import.ordering.noEmptyLine
import shadedForDelta.org.apache.iceberg.hive.HiveCatalog

import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier => SparkTableIdentifier}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.CalendarInterval

object IcebergTransactionUtils
    extends DeltaLogging
  {

  /////////////////
  // Public APIs //
  /////////////////

  def createPartitionSpec(
      icebergSchema: IcebergSchema,
      partitionColumns: Seq[String]): PartitionSpec = {
    if (partitionColumns.isEmpty) {
      PartitionSpec.unpartitioned
    } else {
      val builder = PartitionSpec.builderFor(icebergSchema)
      for (partitionName <- partitionColumns) {
        builder.identity(partitionName)
      }
      builder.build()
    }
  }

  def convertDeltaAddFileToIcebergDataFile(
      add: AddFile,
      tablePath: Path,
      partitionSpec: PartitionSpec,
      logicalToPhysicalPartitionNames: Map[String, String],
      statsSchema: StructType,
      statsParser: String => InternalRow,
      deltaLog: DeltaLog): DataFile = {
    if (add.deletionVector != null) {
      throw new UnsupportedOperationException("No support yet for DVs")
    }

    var dataFileBuilder =
      convertFileAction(add, tablePath, partitionSpec, logicalToPhysicalPartitionNames)
        // Attempt to attach the number of records metric regardless of whether the Delta stats
        // string is null/empty or not because this metric is required by Iceberg. If the number
        // of records is both unavailable here and unavailable in the Delta stats, Iceberg will
        // throw an exception when building the data file.
        .withRecordCount(add.numLogicalRecords.getOrElse(-1L))

    if (add.stats != null && add.stats.nonEmpty) {
      try {
        val statsRow = statsParser(add.stats)

        val metricsConverter = IcebergStatsConverter(statsRow, statsSchema)
        val metrics = new Metrics(
          metricsConverter.numRecordsStat, // rowCount
          null, // columnSizes
          null, // valueCounts
          metricsConverter.nullValueCountsStat.getOrElse(null).asJava, // nullValueCounts
          null, // nanValueCounts
          metricsConverter.lowerBoundsStat.getOrElse(null).asJava, // lowerBounds
          metricsConverter.upperBoundsStat.getOrElse(null).asJava // upperBounds
        )

        dataFileBuilder = dataFileBuilder.withMetrics(metrics)
      } catch {
        case NonFatal(e) =>
          logWarning("Failed to convert Delta stats to Iceberg stats. Iceberg conversion will " +
          "attempt to proceed without stats.", e)
      }
    }

    dataFileBuilder.build()
  }

  /**
   * Note that APIs like [[shadedForDelta.org.apache.iceberg.OverwriteFiles#deleteFile]] take
   * a DataFile, and not a DeleteFile as you might have expected.
   */
  def convertDeltaRemoveFileToIcebergDataFile(
      remove: RemoveFile,
      tablePath: Path,
      partitionSpec: PartitionSpec,
      logicalToPhysicalPartitionNames: Map[String, String]): DataFile = {
    convertFileAction(remove, tablePath, partitionSpec, logicalToPhysicalPartitionNames)
      .withRecordCount(remove.numLogicalRecords.getOrElse(0L))
      .build()
  }

  /**
   * We expose this as a public API since APIs like
   * [[shadedForDelta.org.apache.iceberg.DeleteFiles#deleteFile]] actually only need to take in
   * a file path String, thus we don't need to actually convert a [[RemoveFile]] into a [[DataFile]]
   * in this case.
   */
  def canonicalizeFilePath(f: FileAction, tablePath: Path): String = {
    // Recall that FileActions can have either relative paths or absolute paths (i.e. from shallow-
    // cloned files).
    // Iceberg spec requires path be fully qualified path, suitable for constructing a Hadoop Path
    if (f.pathAsUri.isAbsolute) f.path else new Path(tablePath, f.path).toString
  }

  /** Returns the (deletions, additions) iceberg table property changes. */
  def detectPropertiesChange(
      newProperties: Map[String, String],
      prevPropertiesOpt: Map[String, String]): (Set[String], Map[String, String]) = {
    val newPropertiesIcebergOnly = getIcebergPropertiesFromDeltaProperties(newProperties)
    val prevPropertiesOptIcebergOnly = getIcebergPropertiesFromDeltaProperties(prevPropertiesOpt)

    if (prevPropertiesOptIcebergOnly == newPropertiesIcebergOnly) return (Set.empty, Map.empty)

    (
      prevPropertiesOptIcebergOnly.keySet.diff(newPropertiesIcebergOnly.keySet),
      newPropertiesIcebergOnly
    )
  }

  /**
   * Only keep properties whose key starts with "delta.universalformat.config.iceberg"
   * and strips the prefix from the key; Note the key is already normalized to lower case.
   */
  def getIcebergPropertiesFromDeltaProperties(
      properties: Map[String, String]): Map[String, String] = {
    val additionalPropertyFromDelta = additionalIcebergPropertiesFromDeltaProperties(properties)
    val prefix = DeltaConfigs.DELTA_UNIVERSAL_FORMAT_ICEBERG_CONFIG_PREFIX
    val specifiedProperty =
      properties.filterKeys(_.startsWith(prefix)).map(kv => (kv._1.stripPrefix(prefix), kv._2))
      .toMap
    validateIcebergProperty(additionalPropertyFromDelta, specifiedProperty)
    additionalPropertyFromDelta ++ specifiedProperty
  }

  /** Returns the mapping of logicalPartitionColName -> physicalPartitionColName */
  def getPartitionPhysicalNameMapping(partitionSchema: StructType): Map[String, String] = {
    partitionSchema.fields.map(f => f.name -> DeltaColumnMapping.getPhysicalName(f)).toMap
  }

  ////////////////////
  // Helper Methods //
  ////////////////////

  /** Visible for testing. */
  private[delta] def convertFileAction(
      f: FileAction,
      tablePath: Path,
      partitionSpec: PartitionSpec,
      logicalToPhysicalPartitionNames: Map[String, String]): DataFiles.Builder = {
    val absPath = canonicalizeFilePath(f, tablePath)

    var builder = DataFiles
      .builder(partitionSpec)
      .withPath(absPath)
      .withFileSizeInBytes(f.getFileSize)
      .withFormat(FileFormat.PARQUET)

    if (partitionSpec.isPartitioned) {
      val ICEBERG_NULL_PARTITION_VALUE = "__HIVE_DEFAULT_PARTITION__"
      val partitionPath = partitionSpec
        .fields()
        .asScala
        .map(_.name)
        .map { logicalPartCol =>
          // The Iceberg Schema and PartitionSpec all use the column logical names.
          // Delta FileAction::partitionValues, however, uses physical names.
          val physicalPartKey = logicalToPhysicalPartitionNames(logicalPartCol)

          // ICEBERG_NULL_PARTITION_VALUE is referred in Iceberg lib to mark NULL partition value
          val partValue = Option(f.partitionValues(physicalPartKey))
            .getOrElse(ICEBERG_NULL_PARTITION_VALUE)
          s"$logicalPartCol=$partValue"
        }
        .mkString("/")

      builder = builder.withPartitionPath(partitionPath)
    }

    builder
  }

  /**
   * Create an Iceberg HiveCatalog
   * @param conf: Hadoop Configuration
   * @return
   */
  def createHiveCatalog(conf : Configuration) : HiveCatalog = {
    val catalog = new HiveCatalog()
    catalog.setConf(conf)
    catalog.initialize("spark_catalog", Map.empty[String, String].asJava)
    catalog
  }

  /**
   * Encode Spark table identifier to Iceberg table identifier by putting
   * only "database" to the "namespace" in Iceberg table identifier.
   * See [[HiveCatalog.isValidateNamespace]]
   */
  def convertSparkTableIdentifierToIcebergHive(
      identifier: SparkTableIdentifier): IcebergTableIdentifier = {
    val namespace = (identifier.database) match {
      case Some(database) => Namespace.of(database)
      case _ => Namespace.empty()
    }
    IcebergTableIdentifier.of(namespace, identifier.table)
  }

  // Additional iceberg properties inferred from delta properties
  // If user doesn't specify the property in iceberg table, we infer it from delta properties
  // Otherwise, we validate the user specified property with the inferred property
  // Here's a list of additional properties:
  // 1. iceberg's history.expire.max-snapshot-age-ms:
  //  inferred as min of delta.logRetentionDuration and delta.deletedFileRetentionDuration
  private def additionalIcebergPropertiesFromDeltaProperties(
      properties: Map[String, String]): Map[String, String] = {
    icebergRetentionPropertyFromDelta(properties)
  }

  private def icebergRetentionPropertyFromDelta(
      deltaProperties: Map[String, String]): Map[String, String] = {
    val icebergSnapshotRetentionFromDelta = deltaRetentionMsFrom(deltaProperties)
    lazy val icebergDefault = TableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT
    icebergSnapshotRetentionFromDelta.map { retentionMs =>
      Map(TableProperties.MAX_SNAPSHOT_AGE_MS -> (retentionMs min icebergDefault).toString)
    }.getOrElse(Map.empty)
  }

  // Given additional iceberg property constrained/inferred by Delta and
  // user specified iceberg property, validate that they don't conflict
  private def validateIcebergProperty(
      additionalPropertyFromDelta: Map[String, String],
      customizedProperty: Map[String, String]): Unit = {
    validateIcebergRetentionWithDelta(additionalPropertyFromDelta, customizedProperty)
  }

  // Validation:
  // Customized iceberg retention should be <= to the delta retention
  // Which is min of logRetentionDuration and deletedFileRetentionDuration
  private def validateIcebergRetentionWithDelta(
      additionalPropertyFromDelta: Map[String, String],
      usrSpecifiedProperty: Map[String, String]): Unit = {
    lazy val defaultRetentionDelta =
      calendarStrToMs(DeltaConfigs.LOG_RETENTION.defaultValue) min
      calendarStrToMs(DeltaConfigs.TOMBSTONE_RETENTION.defaultValue)
    lazy val retentionMsFromDelta = additionalPropertyFromDelta
      .getOrElse(TableProperties.MAX_SNAPSHOT_AGE_MS, s"$defaultRetentionDelta").toLong

    usrSpecifiedProperty.get(TableProperties.MAX_SNAPSHOT_AGE_MS).foreach { proposedMs =>
      if (proposedMs.toLong > retentionMsFromDelta) {
        throw new IllegalArgumentException(
          s"Uniform iceberg's ${TableProperties.MAX_SNAPSHOT_AGE_MS} should be set >= " +
          s" min of delta's ${DeltaConfigs.LOG_RETENTION.key} and" +
          s" ${DeltaConfigs.TOMBSTONE_RETENTION.key}." +
          s" Current delta retention min in MS: $retentionMsFromDelta," +
          s" Proposed iceberg retention in Ms: $proposedMs")
      }
    }
  }

  private def deltaRetentionMsFrom(deltaProperties: Map[String, String]): Option[Long] = {
    def getCalendarMsFrom(
        conf: DeltaConfig[CalendarInterval], properties: Map[String, String]): Option[Long] = {
      properties.get(conf.key).map(calendarStrToMs)
    }

    def minOf(a: Option[Long], b: Option[Long]): Option[Long] = (a, b) match {
      case (Some(a), Some(b)) => Some(a min b)
      case (a, b) => a orElse b
    }

    val logRetention = getCalendarMsFrom(DeltaConfigs.LOG_RETENTION, deltaProperties)
    val vacuumRetention = getCalendarMsFrom(DeltaConfigs.TOMBSTONE_RETENTION, deltaProperties)
    minOf(logRetention, vacuumRetention)
  }

  // Converts a string in calendar interval format to milliseconds
  private def calendarStrToMs(calendarStr: String): Long = {
    val interval = parseCalendarInterval(calendarStr)
    DeltaConfigs.getMilliSeconds(interval)
  }
}
