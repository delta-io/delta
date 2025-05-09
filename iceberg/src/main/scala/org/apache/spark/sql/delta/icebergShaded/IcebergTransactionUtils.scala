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

import java.nio.ByteBuffer
import java.time.Instant
import java.time.format.DateTimeParseException

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaErrors, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.PartitionUtils.{timestampPartitionPattern, utcFormatter}
import org.apache.spark.sql.delta.util.TimestampFormatter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import shadedForDelta.org.apache.iceberg.{DataFile, DataFiles, FileFormat, PartitionSpec, Schema => IcebergSchema}
import shadedForDelta.org.apache.iceberg.Metrics
import shadedForDelta.org.apache.iceberg.StructLike
import shadedForDelta.org.apache.iceberg.catalog.{Namespace, TableIdentifier => IcebergTableIdentifier}
import shadedForDelta.org.apache.iceberg.hive.HiveCatalog
import shadedForDelta.org.apache.iceberg.util.DateTimeUtil

import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier => SparkTableIdentifier}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}

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
    if (f.pathAsUri.isAbsolute) f.path else new Path(tablePath, f.toPath.toString).toString
  }

  /** Returns the (deletions, additions) iceberg table property changes. */
  def detectPropertiesChange(
      newProperties: Map[String, String],
      prevProperties: Map[String, String]): (Set[String], Map[String, String]) = {
    val newPropertiesIcebergOnly = DeltaToIcebergConvert.TableProperties(newProperties)
    val prevPropertiesOptIcebergOnly =
      DeltaToIcebergConvert.TableProperties(prevProperties)

    if (prevPropertiesOptIcebergOnly == newPropertiesIcebergOnly) return (Set.empty, Map.empty)

    (
      prevPropertiesOptIcebergOnly.keySet.diff(newPropertiesIcebergOnly.keySet),
      newPropertiesIcebergOnly
    )
  }

  /** Returns the mapping of logicalPartitionColName -> physicalPartitionColName */
  def getPartitionPhysicalNameMapping(partitionSchema: StructType): Map[String, String] = {
    partitionSchema.fields.map(f => f.name -> DeltaColumnMapping.getPhysicalName(f)).toMap
  }

  class Row (val values: Array[Any]) extends StructLike {
    override def size: Int = values.length
    override def get[T <: Any](pos: Int, javaClass: Class[T]): T = javaClass.cast(values(pos))
    override def set[T <: Any](pos: Int, value: T): Unit = {
      values(pos) = value
    }
  }

  ////////////////////
  // Helper Methods //
  ////////////////////

  /** Visible for testing. */
  private[delta] def convertDeltaAddFileToIcebergDataFile(
      add: AddFile,
      tablePath: Path,
      partitionSpec: PartitionSpec,
      logicalToPhysicalPartitionNames: Map[String, String],
      statsParser: String => InternalRow,
      snapshot: Snapshot): DataFile = {
    if (add.deletionVector != null) {
      throw new UnsupportedOperationException("No support yet for DVs")
    }

    var dataFileBuilder =
      convertFileAction(
        add, tablePath, partitionSpec, logicalToPhysicalPartitionNames, snapshot)
        // Attempt to attach the number of records metric regardless of whether the Delta stats
        // string is null/empty or not because this metric is required by Iceberg. If the number
        // of records is both unavailable here and unavailable in the Delta stats, Iceberg will
        // throw an exception when building the data file.
        .withRecordCount(add.numLogicalRecords.getOrElse(-1L))

    try {
      if (add.stats != null && add.stats.nonEmpty) {
        dataFileBuilder = dataFileBuilder.withMetrics(
          getMetricsForIcebergDataFile(statsParser, add.stats, snapshot.statsSchema))
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to convert Delta stats to Iceberg stats. Iceberg conversion will " +
          "attempt to proceed without stats.", e)
    }

    dataFileBuilder.build()
  }

  private[delta] def convertDeltaRemoveFileToIcebergDataFile(
      remove: RemoveFile,
      tablePath: Path,
      partitionSpec: PartitionSpec,
      logicalToPhysicalPartitionNames: Map[String, String],
      snapshot: Snapshot): DataFile = {
    convertFileAction(
      remove, tablePath, partitionSpec, logicalToPhysicalPartitionNames, snapshot)
      .withRecordCount(remove.numLogicalRecords.getOrElse(0L))
      .build()
  }

  private[delta] def convertFileAction(
      f: FileAction,
      tablePath: Path,
      partitionSpec: PartitionSpec,
      logicalToPhysicalPartitionNames: Map[String, String],
      snapshot: Snapshot): DataFiles.Builder = {
    val absPath = canonicalizeFilePath(f, tablePath)
    var builder = DataFiles
      .builder(partitionSpec)
      .withPath(absPath)
      .withFileSizeInBytes(f.getFileSize)
      .withFormat(FileFormat.PARQUET)
    if (partitionSpec.isPartitioned) {
      builder = builder.withPartition(
        DeltaToIcebergConvert.Partition.convertPartitionValues(
          snapshot, partitionSpec, f.partitionValues, logicalToPhysicalPartitionNames))
    }
    builder
  }

  private lazy val timestampFormatter =
    TimestampFormatter(timestampPartitionPattern, java.util.TimeZone.getDefault)

  /**
   * Follows deserialization as specified here
   * https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Partition-Value-Serialization
   */
  private[delta] def stringToIcebergPartitionValue(
      elemType: DataType,
      partitionVal: String,
      version: Long): Any = {
    if (partitionVal == null || partitionVal == "__HIVE_DEFAULT_PARTITION__") {
      return null
    }

    elemType match {
      case _: StringType => partitionVal
      case _: DateType =>
        java.sql.Date.valueOf(partitionVal).toLocalDate.toEpochDay.asInstanceOf[Int]
      case _: IntegerType => partitionVal.toInt.asInstanceOf[Integer]
      case _: ShortType => partitionVal.toInt.asInstanceOf[Integer]
      case _: ByteType => partitionVal.toInt.asInstanceOf[Integer]
      case _: LongType => partitionVal.toLong
      case _: BooleanType => partitionVal.toBoolean
      case _: FloatType => partitionVal.toFloat
      case _: DoubleType => partitionVal.toDouble
      case _: DecimalType => new java.math.BigDecimal(partitionVal)
      case _: BinaryType => ByteBuffer.wrap(partitionVal.getBytes("UTF-8"))
      case _: TimestampNTZType =>
        DateTimeUtil.isoTimestampToMicros(
          partitionVal.replace(" ", "T"))
      case _: TimestampType =>
        try {
          getMicrosSinceEpoch(partitionVal)
        } catch {
          case _: DateTimeParseException =>
            // In case of non-ISO timestamps, parse and interpret the timestamp as system time
            // and then convert to UTC
            val utcInstant = utcFormatter.format(timestampFormatter.parse(partitionVal))
            getMicrosSinceEpoch(utcInstant)
        }
      case _ =>
        throw DeltaErrors.universalFormatConversionFailedException(
          version, "iceberg", "Unexpected partition data type " + elemType)
    }
  }

  private def getMicrosSinceEpoch(instant: String): Long = {
    DateTimeUtil.microsFromInstant(
      Instant.parse(instant))
  }

  private def getMetricsForIcebergDataFile(
      statsParser: String => InternalRow,
      stats: String,
      statsSchema: StructType): Metrics = {
    val statsRow = statsParser(stats)
    val metricsConverter = IcebergStatsConverter(statsRow, statsSchema)
    new Metrics(
      metricsConverter.numRecordsStat, // rowCount
      null, // columnSizes
      null, // valueCounts
      metricsConverter.nullValueCountsStat.getOrElse(null).asJava, // nullValueCounts
      null, // nanValueCounts
      metricsConverter.lowerBoundsStat.getOrElse(null).asJava, // lowerBounds
      metricsConverter.upperBoundsStat.getOrElse(null).asJava // upperBounds
    )
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
}
