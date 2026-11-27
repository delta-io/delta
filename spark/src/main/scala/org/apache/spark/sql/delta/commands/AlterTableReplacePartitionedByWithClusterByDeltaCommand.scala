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

package org.apache.spark.sql.delta.commands

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{Action, Metadata}
import org.apache.spark.sql.delta.amt.AMTUtils
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.{DeltaStatistics, SkippingEligibleDataType, StatisticsCollection}
import org.apache.spark.sql.delta.util.{DeltaFileOperations, JsonUtils}
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature
import com.fasterxml.jackson.databind.node.{DecimalNode, LongNode, ObjectNode, TextNode}
import org.apache.parquet.format.converter.ParquetMetadataConverter

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.parquet.ParquetFooterReaderShims
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/**
 * Command that replaces the partitioning of a Delta table with clustering, without rewriting
 * any data file:
 *  - ALTER TABLE .. REPLACE PARTITIONED BY WITH CLUSTER BY (col1, col2, ...)
 *  - ALTER TABLE .. REPLACE PARTITIONED BY WITH CLUSTER BY NONE
 *  - ALTER TABLE .. REPLACE PARTITIONED BY WITH CLUSTER BY, which clusters the table by its
 *    partition columns (`clusterByPartitionColumns`)
 *
 * The conversion is a single metadata-only commit which
 *  1. clears the partition columns in the table metadata,
 *  2. re-adds every active file with empty `partitionValues` and `dataChange = false`, keeping its
 *     path, deletion vector and row tracking fields. Per-file min/max/nullCount statistics are
 *     synthesized for the former partition columns from the (constant) partition values so that
 *     data skipping on these columns keeps working, and
 *  3. when clustering columns are given, enables clustering and records the clustering columns.
 *
 * After the conversion the former partition columns are read from the data files, so they must be
 * physically stored in every data file (see `delta.writePartitionColumnsToParquet` and the
 * `materializePartitionColumns` table feature). This is verified by reading the Parquet footers
 * before committing.
 *
 * When a former partition column would not get statistics anymore, because it is after the first
 * `delta.dataSkippingNumIndexedCols` columns or not in `delta.dataSkippingStatsColumns`, the
 * conversion sets `delta.dataSkippingStatsColumns` so that the columns that had statistics before
 * and the former partition columns keep them.
 *
 * As for any change of the partitioning, streaming queries reading the table fail when they reach
 * the conversion. A query with a `schemaTrackingLocation` continues after a restart; other queries
 * must be restarted with a new checkpoint.
 *
 * The commit is done with [[OptimisticTransaction.commitLarge]], which fails if any other commit
 * happened since the transaction started. Hence no file can be added concurrently between the
 * verification and the commit.
 */
case class AlterTableReplacePartitionedByWithClusterByDeltaCommand(
    table: DeltaTableV2,
    requestedClusteringColumns: Seq[Seq[String]],
    clusterByPartitionColumns: Boolean = false)
  extends LeafRunnableCommand with AlterDeltaTableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.alter.replacePartitionedByWithClusterBy") {
      val txn = startTransaction()
      val snapshot = txn.snapshot
      val oldMetadata = txn.metadata
      if (oldMetadata.partitionColumns.isEmpty) {
        throw DeltaErrors.replacePartitionedByOnUnpartitionedTableException()
      }
      // The commit below neither runs the IcebergCompat checks (which do not allow changing the
      // partition columns) nor converts the table to Iceberg or Hudi, and the adaptive metadata
      // manifest of the files records their partitioning.
      IcebergCompat.anyEnabled(oldMetadata).foreach { compat =>
        throw DeltaErrors.replacePartitionedByUnsupportedTableFeatureException(compat.config.key)
      }
      if (UniversalFormat.icebergEnabled(oldMetadata) || UniversalFormat.hudiEnabled(oldMetadata)) {
        throw DeltaErrors.replacePartitionedByUnsupportedTableFeatureException(
          DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.key)
      }
      if (AMTUtils.amtEnabled(snapshot, isPreviewCompatible = true)) {
        throw DeltaErrors.replacePartitionedByUnsupportedTableFeatureException(
          AdaptiveMetadataTableFeatureBase.supportedBy(snapshot.protocol)
            .getOrElse(AdaptiveMetadataTableFeature).name)
      }
      val clusteringColumns = if (clusterByPartitionColumns) {
        oldMetadata.partitionColumns.map(Seq(_))
      } else {
        requestedClusteringColumns
      }
      ClusteredTableUtils.validateNumClusteringColumns(clusteringColumns, Some(deltaLog))

      val physicalPartitionSchema = oldMetadata.physicalPartitionSchema
      val conf = sparkSession.sessionState.conf

      // Update the metadata and validate the clustering columns before reading the footers of all
      // data files below, so that invalid commands fail fast.
      val newLogicalClusteringColumns = clusteringColumns.map(FieldReference(_).toString)
      val baseConfiguration = oldMetadata.configuration
      val clusteringConfiguration =
        if (clusteringColumns.nonEmpty && !ClusteredTableUtils.isSupported(txn.protocol)) {
          ClusteredTableUtils.getTableFeatureProperties(baseConfiguration)
        } else {
          Map.empty[String, String]
        }
      val unpartitionedMetadata = oldMetadata.copy(
        partitionColumns = Nil, configuration = baseConfiguration ++ clusteringConfiguration)
      val statsColumnsConfiguration =
        ReplacePartitionedByUtils.statsColumnsConfiguration(oldMetadata, unpartitionedMetadata)
      txn.updateMetadata(unpartitionedMetadata.copy(
        configuration = unpartitionedMetadata.configuration ++ statsColumnsConfiguration))

      val domainMetadata = if (clusteringColumns.nonEmpty) {
        ClusteredTableUtils.validateClusteringColumnsInStatsSchema(
          txn.protocol, txn.metadata, ClusterBySpec(clusteringColumns))
        ClusteredTableUtils.getClusteringDomainMetadataForAlterTableClusterBy(
          newLogicalClusteringColumns, txn)
      } else {
        Nil
      }

      ReplacePartitionedByUtils.verifyPartitionColumnsMaterialized(
        sparkSession, deltaLog, snapshot, oldMetadata.partitionColumns,
        physicalPartitionSchema.fieldNames.toSeq)

      val stringPrefixLength = conf.getConf(DeltaSQLConf.DATA_SKIPPING_STRING_PREFIX_LENGTH)
      val timeZoneId = conf.sessionLocalTimeZone
      val partitionFields = physicalPartitionSchema.fields.toSeq
      import org.apache.spark.sql.delta.implicits._
      val newAddFiles = snapshot.allFiles.mapPartitions { files =>
        files.map { file =>
          file.copy(
            partitionValues = Map.empty,
            dataChange = false,
            stats = ReplacePartitionedByUtils.addPartitionValuesToStats(
              file.stats, file.partitionValues, partitionFields, stringPrefixLength, timeZoneId))
        }
      }

      val actions: Iterator[Action] =
        newAddFiles.toLocalIterator().asScala ++ domainMetadata.iterator
      val newProtocolOpt =
        if (txn.protocol != snapshot.protocol) Some(txn.protocol) else None
      txn.commitLarge(
        sparkSession,
        actions,
        newProtocolOpt,
        DeltaOperations.ReplacePartitionedByWithClusterBy(
          oldMetadata.partitionColumns.mkString(","),
          newLogicalClusteringColumns.mkString(",")),
        context = Map.empty,
        metrics = Map.empty,
        dataChange = false)
    }
    Seq.empty[Row]
  }
}

object ReplacePartitionedByUtils {

  private val NUM_EXAMPLE_FILES = 5

  /**
   * Verifies that every active data file of the snapshot physically stores all the given
   * partition columns, by reading the Parquet footers of the files.
   */
  def verifyPartitionColumnsMaterialized(
      spark: SparkSession,
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      logicalPartitionColumns: Seq[String],
      physicalPartitionColumns: Seq[String]): Unit = {
    val broadcastConf = spark.sparkContext.broadcast(
      new SerializableConfiguration(deltaLog.newDeltaHadoopConf()))
    val dataRootDir = deltaLog.dataPath.toString
    val requiredColumns = physicalPartitionColumns.map(_.toLowerCase(Locale.ROOT))

    import org.apache.spark.sql.delta.implicits._
    val filesMissingColumns = snapshot.allFiles.select("path").as[String].mapPartitions { paths =>
      val conf = broadcastConf.value.value
      paths.filter { relativePath =>
        val path = DeltaFileOperations.absolutePath(dataRootDir, relativePath)
        val status = path.getFileSystem(conf).getFileStatus(path)
        val footer = ParquetFooterReaderShims.readParquetFooter(
          conf, status, ParquetMetadataConverter.SKIP_ROW_GROUPS)
        val fileColumns = footer.getFileMetaData.getSchema.getFields.asScala
          .map(_.getName.toLowerCase(Locale.ROOT)).toSet
        !requiredColumns.forall(fileColumns.contains)
      }
    }

    val examples = filesMissingColumns.take(NUM_EXAMPLE_FILES)
    if (examples.nonEmpty) {
      throw DeltaErrors.replacePartitionedByPartitionColumnsNotMaterializedException(
        logicalPartitionColumns, filesMissingColumns.count(), examples.toSeq)
    }
  }

  /**
   * Returns the table configuration that keeps statistics on the former partition columns, and on
   * all the columns that had statistics before, once the partitioning is dropped.
   *
   * Partition columns are not data columns, so they do not count towards
   * `delta.dataSkippingNumIndexedCols` and cannot be listed in `delta.dataSkippingStatsColumns`.
   * Once they become data columns, a former partition column after the first indexed columns
   * would get no statistics (the synthesized ones would be ignored too), and a former partition
   * column before the last indexed column would push that column out of the indexed columns. In
   * these cases `delta.dataSkippingStatsColumns` is set to the columns that had statistics before
   * followed by the former partition columns. Top-level columns of a type that does not support
   * data skipping cannot be listed and are left out.
   */
  def statsColumnsConfiguration(
      partitionedMetadata: Metadata,
      unpartitionedMetadata: Metadata): Map[String, String] = {
    def isListable(field: StructField): Boolean =
      field.dataType.isInstanceOf[StructType] || SkippingEligibleDataType(field.dataType)
    def key(path: Seq[String]): Seq[String] = path.map(_.toLowerCase(Locale.ROOT))

    val formerPartitionColumns = partitionedMetadata.partitionSchema.fields.toSeq
      .filter(isListable).map(field => Seq(field.name))
    val indexedBefore = StructType(indexedColumns(partitionedMetadata).fields.filter(isListable))
    val indexedAfter = leafPaths(indexedColumns(unpartitionedMetadata)).map(key).toSet
    val required = leafPaths(indexedBefore) ++ formerPartitionColumns
    if (required.forall(path => indexedAfter.contains(key(path)))) return Map.empty

    val formerPartitionColumnNames = formerPartitionColumns.map(UnresolvedAttribute(_).sql)
    val statsColumns =
      DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.fromMetaData(partitionedMetadata) match {
        case Some(configured) => configured.trim +: formerPartitionColumnNames
        case None =>
          statsColumnNames(indexedBefore, partitionedMetadata.dataSchema)
            .map(UnresolvedAttribute(_).sql) ++ formerPartitionColumnNames
      }
    Map(DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.key ->
      statsColumns.filter(_.nonEmpty).mkString(","))
  }

  /** The logical data schema pruned to the columns that statistics are collected on. */
  private def indexedColumns(metadata: Metadata): StructType = {
    val dataSchema = metadata.dataSchema
    StatisticsCollection.getIndexedColumns(
      SchemaMergingUtils.explodeNestedFieldNames(dataSchema),
      StatisticsCollection.configuredDeltaStatsColumnSpec(metadata),
      dataSchema,
      NoMapping)
  }

  private def leafPaths(schema: StructType, prefix: Seq[String] = Nil): Seq[Seq[String]] =
    schema.fields.toSeq.flatMap { field =>
      field.dataType match {
        case struct: StructType => leafPaths(struct, prefix :+ field.name)
        case _ => Seq(prefix :+ field.name)
      }
    }

  /**
   * Returns the names to list in `delta.dataSkippingStatsColumns` to collect statistics on the
   * columns of `indexed`, a pruned version of `schema`. A struct is listed by its name when none of
   * its fields are pruned.
   */
  private def statsColumnNames(
      indexed: StructType,
      schema: StructType,
      prefix: Seq[String] = Nil): Seq[Seq[String]] =
    indexed.fields.toSeq.flatMap { field =>
      (field.dataType, schema(field.name).dataType) match {
        case (struct: StructType, fullStruct: StructType) if struct != fullStruct =>
          statsColumnNames(struct, fullStruct, prefix :+ field.name)
        case _ => Seq(prefix :+ field.name)
      }
    }

  /**
   * Adds min/max/nullCount statistics for the given partition columns to the stats JSON of a
   * file, using the file's partition values. Every row in a file has the same value for a
   * partition column, so these statistics are exact. Statistics that already exist are kept, and
   * no statistics are added if the file has none.
   *
   * A partition value is parsed the same way as when it is read as a partition value. No
   * statistics are added for a column whose value is an empty string or cannot be cast to the
   * column type, because the value stored in the data file for it is not known. Min/max values are
   * only synthesized for string, integral, decimal and date columns; other types only get a null
   * count.
   */
  // Reads the numbers of the statistics exactly, so that the decimal min/max values of the other
  // columns are not rounded through Double when the statistics are written back.
  private lazy val exactStatsReader = JsonUtils.mapper.reader()
    .`with`(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
    .without(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)

  def addPartitionValuesToStats(
      stats: String,
      partitionValues: Map[String, String],
      partitionFields: Seq[StructField],
      stringPrefixLength: Int,
      timeZoneId: String): String = {
    if (stats == null || stats.isEmpty) return stats
    val root = try {
      exactStatsReader.readTree(stats) match {
        case o: ObjectNode => o
        case _ => return stats
      }
    } catch {
      case _: JsonProcessingException => return stats
    }
    val numRecords = root.get(DeltaStatistics.NUM_RECORDS)
    if (numRecords == null || !numRecords.canConvertToLong) return stats

    def objectField(name: String): Option[ObjectNode] = root.get(name) match {
      case null => Some(root.putObject(name))
      case o: ObjectNode => Some(o)
      case _ => None
    }
    val minValues = objectField(DeltaStatistics.MIN)
    val maxValues = objectField(DeltaStatistics.MAX)
    val nullCounts = objectField(DeltaStatistics.NULL_COUNT)

    partitionFields.foreach { field =>
      val name = field.name
      partitionValues.getOrElse(name, null) match {
        case null =>
          nullCounts.filterNot(_.has(name)).foreach(_.put(name, numRecords.asLong))
        case "" =>
          // The protocol defines an empty string as a null partition value, but Delta reads it as
          // an empty string for string columns, so the file may store either.
        case value =>
          val parsed = Cast(Literal(value), field.dataType, Option(timeZoneId), ansiEnabled = false)
            .eval()
          if (parsed != null) {
            nullCounts.filterNot(_.has(name)).foreach(_.put(name, 0L))
            toStatsValue(parsed, field.dataType, stringPrefixLength).foreach { v =>
              minValues.filterNot(_.has(name)).foreach(_.set[JsonNode](name, v))
              maxValues.filterNot(_.has(name)).foreach(_.set[JsonNode](name, v))
            }
          }
      }
    }
    JsonUtils.mapper.writeValueAsString(root)
  }

  /** Converts a value of the given type, as returned by [[Cast]], to its JSON stats value. */
  private def toStatsValue(value: Any, dataType: DataType, stringPrefixLength: Int)
      : Option[JsonNode] = (dataType, value) match {
    // Longer strings would need to be truncated the same way as collected stats.
    case (StringType, s: UTF8String) if s.numChars <= stringPrefixLength =>
      Some(TextNode.valueOf(s.toString))
    case (ByteType | ShortType | IntegerType | LongType, n: java.lang.Number) =>
      Some(LongNode.valueOf(n.longValue))
    case (_: DecimalType, d: Decimal) => Some(DecimalNode.valueOf(d.toJavaBigDecimal))
    case (DateType, days: Int) =>
      Some(TextNode.valueOf(java.time.LocalDate.ofEpochDay(days.toLong).toString))
    case _ => None
  }
}
