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

package org.apache.spark.sql.delta.stats

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.existentials
import scala.util.control.NonFatal

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaColumnMappingMode, DeltaErrors, IdMapping, NameMapping, NoMapping}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaStatistics._
import org.apache.spark.sql.delta.util.{DeltaFileOperations, JsonUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ParquetMetadata}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation.{DateLogicalTypeAnnotation, StringLogicalTypeAnnotation}
import org.apache.parquet.schema.PrimitiveType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration


object StatsCollectionUtils
  extends Logging
{

  /** A helper function to compute stats of addFiles using StatsCollector.
   *
   * @param spark The SparkSession used to process data.
   * @param conf The Hadoop configuration used to access file system.
   * @param dataPath The data path of table, to which these AddFile(s) belong.
   * @param addFiles The list of target AddFile(s) to be processed.
   * @param columnMappingMode The column mapping mode of table.
   * @param dataSchema The data schema of table.
   * @param statsSchema The stats schema to be collected.
   * @param ignoreMissingStats Whether to ignore missing stats during computation.
   * @param setBoundsToWide Whether to set bounds to wide independently of whether or not
   *                        the files have DVs.
   *
   * @return A list of AddFile(s) with newly computed stats, please note the existing stats from
   *         the input addFiles will be ignored regardless.
   */
  def computeStats(
      spark: SparkSession,
      conf: Configuration,
      dataPath: Path,
      addFiles: Dataset[AddFile],
      columnMappingMode: DeltaColumnMappingMode,
      dataSchema: StructType,
      statsSchema: StructType,
      ignoreMissingStats: Boolean = true,
      setBoundsToWide: Boolean = false): Dataset[AddFile] = {

    import org.apache.spark.sql.delta.implicits._

    val stringTruncateLength =
      spark.sessionState.conf.getConf(DeltaSQLConf.DATA_SKIPPING_STRING_PREFIX_LENGTH)

    val statsCollector = StatsCollector(columnMappingMode, dataSchema, statsSchema,
      ignoreMissingStats, Some(stringTruncateLength))

    val serializableConf = new SerializableConfiguration(conf)
    val broadcastConf = spark.sparkContext.broadcast(serializableConf)

    val dataRootDir = dataPath.toString
    addFiles.mapPartitions { addFileIter =>
      val defaultFileSystem = new Path(dataRootDir).getFileSystem(broadcastConf.value.value)
      addFileIter.map { addFile =>
        val path = DeltaFileOperations.absolutePath(dataRootDir, addFile.path)
        val fileStatus = if (path.toString.startsWith(dataRootDir)) {
          defaultFileSystem.getFileStatus(path)
        } else {
          path.getFileSystem(broadcastConf.value.value).getFileStatus(path)
        }

        val (stats, metric) = statsCollector.collect(
          ParquetFileReader.readFooter(broadcastConf.value.value, fileStatus))

        if (metric.totalMissingFields > 0 || metric.numMissingTypes > 0) {
          logWarning(
            s"StatsCollection of file `$path` misses fields/types: ${JsonUtils.toJson(metric)}")
        }

        val statsWithTightBoundsCol = {
          val hasDeletionVector =
            addFile.deletionVector != null && !addFile.deletionVector.isEmpty
          stats + (TIGHT_BOUNDS -> !(setBoundsToWide || hasDeletionVector))
        }

        addFile.copy(stats = JsonUtils.toJson(statsWithTightBoundsCol))
      }
    }
  }
}

/**
 * A helper class to collect stats of parquet data files for Delta table and its equivalent (tables
 * that can be converted into Delta table like Parquet/Iceberg table).
 *
 * @param dataSchema The data schema from table metadata, which is the logical schema with logical
 *                   to physical mapping per schema field. It is used to map statsSchema to parquet
 *                   metadata.
 * @param statsSchema The schema of stats to be collected, statsSchema should follow the physical
 *                    schema and must be generated by StatisticsCollection.
 * @param ignoreMissingStats Indicate whether to return partial result by ignoring missing stats
 *                           or throw an exception.
 * @param stringTruncateLength The optional max length of string stats to be truncated into.
 *
 * Scala Example:
 * {{{
 * import org.apache.spark.sql.delta.stats.StatsCollector
 *
 * val stringTruncateLength =
 *   spark.sessionState.conf.getConf(DeltaSQLConf.DATA_SKIPPING_STRING_PREFIX_LENGTH)
 *
 * val statsCollector = StatsCollector(
 *   snapshot.metadata.columnMappingMode, snapshot.metadata.dataSchema, snapshot.statsSchema,
 *   ignoreMissingStats = false, Some(stringTruncateLength))
 *
 * val filesWithStats = snapshot.allFiles.map { file =>
 *   val path = DeltaFileOperations.absolutePath(dataPath, file.path)
 *   val fileSystem = path.getFileSystem(hadoopConf)
 *   val fileStatus = fileSystem.listStatus(path).head
 *
 *   val footer = ParquetFileReader.readFooter(hadoopConf, fileStatus)
 *   val (stats, _) = statsCollector.collect(footer)
 *   file.copy(stats = JsonUtils.toJson(stats))
 * }
 * }}}
 */
abstract class StatsCollector(
    dataSchema: StructType,
    statsSchema: StructType,
    ignoreMissingStats: Boolean,
    stringTruncateLength: Option[Int])
  extends Serializable
{

  final val NUM_MISSING_TYPES = "numMissingTypes"

  /**
   * Used to report number of missing fields per supported type and number of missing unsupported
   * types in the collected statistics, currently the statistics collection supports 4 types of
   * stats: NUM_RECORDS, MAX, MIN, NULL_COUNT.
   *
   * @param numMissingMax The number of missing fields for MAX
   * @param numMissingMin The number of missing fields for MIN
   * @param numMissingNullCount The number of missing fields for NULL_COUNT
   * @param numMissingTypes The number of unsupported type being requested.
   */
  case class StatsCollectionMetrics(
      numMissingMax: Long,
      numMissingMin: Long,
      numMissingNullCount: Long,
      numMissingTypes: Long) {

    val totalMissingFields: Long = Seq(numMissingMax, numMissingMin, numMissingNullCount).sum
  }

  object StatsCollectionMetrics {
    def apply(missingFieldCounts: Map[String, Long]): StatsCollectionMetrics = {
      StatsCollectionMetrics(
        missingFieldCounts.getOrElse(MAX, 0L),
        missingFieldCounts.getOrElse(MIN, 0L),
        missingFieldCounts.getOrElse(NULL_COUNT, 0L),
        missingFieldCounts.getOrElse(NUM_MISSING_TYPES, 0L))
    }
  }

  /**
   * A list of schema physical path and corresponding struct field of leaf fields. Beside primitive
   * types, Map and Array (instead of their sub-columns) are also treated as leaf fields since we
   * only compute null count of them, and null is counted based on themselves instead of sub-fields.
   */
  protected lazy val schemaPhysicalPathAndSchemaField: Seq[(Seq[String], StructField)] = {
    def explode(schema: StructType): Seq[(Seq[String], StructField)] = {
      schema.flatMap { field =>
        val physicalName = DeltaColumnMapping.getPhysicalName(field)
        field.dataType match {
          case s: StructType =>
            explode(s).map { case (path, field) => (Seq(physicalName) ++ path, field) }
          case _ => (Seq(physicalName), field) :: Nil
        }
      }
    }
    explode(dataSchema)
  }

  /**
   * Returns the map from schema physical field path (field for which to collect stats) to the
   * parquet metadata column index (where to collect stats). statsSchema generated by
   * StatisticsCollection always use physical field paths so physical field paths are the same as
   * to the ones used in statsSchema. Child class must implement this method based on delta column
   * mapping mode.
   */
  def getSchemaPhysicalPathToParquetIndex(blockMetaData: BlockMetaData): Map[Seq[String], Int]

  /**
   * Collects the stats from [[ParquetMetadata]]
   *
   * @param parquetMetadata The metadata of parquet file following physical schema, it contains
   *                        statistics of row groups.
   *
   * @return A nested Map[String: Any] from requested stats field names to their stats field value
   *         and [[StatsCollectionMetrics]] counting the number of missing fields/types.
   */
  final def collect(
      parquetMetadata: ParquetMetadata): (Map[String, Any], StatsCollectionMetrics) = {
    val blocks = parquetMetadata.getBlocks.asScala.toSeq
    if (blocks.isEmpty) {
      return (Map(NUM_RECORDS -> 0L), StatsCollectionMetrics(Map.empty[String, Long]))
    }

    val schemaPhysicalPathToParquetIndex = getSchemaPhysicalPathToParquetIndex(blocks.head)
    val missingFieldCounts =
      mutable.Map(MAX -> 0L, MIN -> 0L, NULL_COUNT -> 0L, NUM_MISSING_TYPES -> 0L)

    // Collect the actual stats.
    //
    // The result of this operation is a tree of maps that matches the structure of the stats
    // schema. The stats schema is split by stats type at the top, and each type matches the
    // structure of the data schema (can be subset), so we collect per stats type. E.g. the MIN
    // values are under MIN.a, MIN.b.c, MIN.b.d etc., and then the MAX values are under MAX.a,
    // MAX.b.c etc. Note, we do omit here the tightBounds column and add it at a later stage.
    val collectedStats = statsSchema.filter(_.name != TIGHT_BOUNDS).map {
      case StructField(NUM_RECORDS, LongType, _, _) =>
        val numRecords = blocks.map { block =>
          block.getRowCount
        }.sum
        NUM_RECORDS -> numRecords
      case StructField(MIN, statsTypeSchema: StructType, _, _) =>
        val (minValues, numMissingFields) =
          collectStats(Seq.empty[String], statsTypeSchema, blocks, schemaPhysicalPathToParquetIndex,
            ignoreMissingStats)(aggMaxOrMin(isMax = false))
        missingFieldCounts(MIN) += numMissingFields
        MIN -> minValues
      case StructField(MAX, statsTypeSchema: StructType, _, _) =>
        val (maxValues, numMissingFields) =
          collectStats(Seq.empty[String], statsTypeSchema, blocks, schemaPhysicalPathToParquetIndex,
            ignoreMissingStats)(aggMaxOrMin(isMax = true))
        missingFieldCounts(MAX) += numMissingFields
        MAX -> maxValues
      case StructField(NULL_COUNT, statsTypeSchema: StructType, _, _) =>
        val (nullCounts, numMissingFields) =
          collectStats(Seq.empty[String], statsTypeSchema, blocks, schemaPhysicalPathToParquetIndex,
            ignoreMissingStats)(aggNullCount)
        missingFieldCounts(NULL_COUNT) += numMissingFields
        NULL_COUNT -> nullCounts
      case field: StructField =>
        if (ignoreMissingStats) {
          missingFieldCounts(NUM_MISSING_TYPES) += 1
          field.name -> Map.empty[String, Any]
        } else {
          throw new UnsupportedOperationException(s"stats type not supported: ${field.name}")
        }
    }.toMap

    (collectedStats, StatsCollectionMetrics(missingFieldCounts.toMap))
  }

  /**
   * Collects statistics by recurring through the structure of statsSchema and tracks the fields
   * that we have seen so far in parentPhysicalPath.
   *
   * @param parentPhysicalFieldPath The absolute path of parent field with physical names.
   * @param statsSchema The schema with physical names to collect stats recursively.
   * @param blocks The metadata of Parquet row groups, which contains the raw stats.
   * @param schemaPhysicalPathToParquetIndex Map from schema path to parquet metadata column index.
   * @param ignoreMissingStats Whether to ignore and log missing fields or throw an exception.
   * @param aggFunc The aggregation function used to aggregate stats across row.
   *
   * @return A nested Map[String: Any] from schema field name to stats value and a count of missing
   *         fields.
   *
   * Here is an example of stats:
   *
   * stats schema:
   * | -- id: INT
   * | -- person: STRUCT
   *     | name: STRUCT
   *         | -- first: STRING
   *         | -- last: STRING
   *     | height: LONG
   *
   * The stats:
   * Map(
   *   "id" -> 1003,
   *   "person" -> Map(
   *      "name" -> Map(
   *         "first" -> "Chris",
   *         "last" -> "Green"
   *       ),
   *       "height" -> 175L
   *   )
   * )
   */
  private def collectStats(
      parentPhysicalFieldPath: Seq[String],
      statsSchema: StructType,
      blocks: Seq[BlockMetaData],
      schemaPhysicalPathToParquetIndex: Map[Seq[String], Int],
      ignoreMissingStats: Boolean)(
      aggFunc: (Seq[BlockMetaData], Int) => Any): (Map[String, Any], Long) = {
    val stats = mutable.Map.empty[String, Any]
    var numMissingFields = 0L
    statsSchema.foreach {
      case StructField(name, dataType: StructType, _, _) =>
        val (map, numMissingFieldsInSubtree) =
          collectStats(parentPhysicalFieldPath :+ name, dataType, blocks,
            schemaPhysicalPathToParquetIndex, ignoreMissingStats)(aggFunc)
        numMissingFields += numMissingFieldsInSubtree
        if (map.nonEmpty) {
          stats += name -> map
        }
      case StructField(name, _, _, _) =>
        val physicalFieldPath = parentPhysicalFieldPath :+ name
        if (schemaPhysicalPathToParquetIndex.contains(physicalFieldPath)) {
          try {
            val value = aggFunc(blocks, schemaPhysicalPathToParquetIndex(physicalFieldPath))
            // None value means the stats is undefined for this field (e.g., max/min of a field,
            // whose values are nulls in all blocks), we use null to be consistent with stats
            // generated from SQL.
            if (value != None) {
              stats += name -> value
            } else {
              stats += name -> null
            }
          } catch {
            case NonFatal(_) if ignoreMissingStats => numMissingFields += 1L
            case exception: Throwable => throw exception
          }
        } else if (ignoreMissingStats) {
          // Physical field path requested by stats is missing in the mapping, so it's missing from
          // the parquet metadata.
          numMissingFields += 1L
        } else {
          val columnPath = physicalFieldPath.mkString("[", ", ", "]")
          throw DeltaErrors.deltaStatsCollectionColumnNotFound("all", columnPath)
        }
    }

    (stats.toMap, numMissingFields)
  }

  /** The aggregation function used to collect max and min */
  private def aggMaxOrMin(isMax: Boolean)(blocks: Seq[BlockMetaData], index: Int): Any = {
    val columnMetadata = blocks.head.getColumns.get(index)
    val primitiveType = columnMetadata.getPrimitiveType
    // Physical type of timestamp is INT96 in both Parquet and Delta.
    if (primitiveType.getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.INT96) {
      throw new UnsupportedOperationException(
        s"max/min stats is not supported for INT96 timestamp: ${columnMetadata.getPath}")
    }

    var aggregatedValue: Any = None
    blocks.foreach { block =>
      val column = block.getColumns.get(index)
      val statistics = column.getStatistics
      // Skip this block if the column has null for all rows, stats is defined as long as it exists
      // in even a single block.
      if (statistics.hasNonNullValue) {
        val currentValue = if (isMax) statistics.genericGetMax else statistics.genericGetMin
        if (currentValue == null) {
          throw DeltaErrors.deltaStatsCollectionColumnNotFound("max/min", column.getPath.toString)
        }

        if (aggregatedValue == None) {
          aggregatedValue = currentValue
        } else {
          // TODO: check NaN value for floating point columns.
          val compareResult = currentValue.asInstanceOf[Comparable[Any]].compareTo(aggregatedValue)
          if ((isMax && compareResult > 0) || (!isMax && compareResult < 0)) {
            aggregatedValue = currentValue
          }
        }
      }
    }

    val logicalType = primitiveType.getLogicalTypeAnnotation
    aggregatedValue match {
      case bytes: Binary if logicalType.isInstanceOf[StringLogicalTypeAnnotation] =>
        val rawString = bytes.toStringUsingUTF8
        if (stringTruncateLength.isDefined && rawString.length > stringTruncateLength.get) {
          if (isMax) {
            // Append tie breakers to assure that any value in this column is less than or equal to
            // the max, check the helper function for more details.
            StatisticsCollection.truncateMaxStringAgg(stringTruncateLength.get)(rawString)
          } else {
            rawString.substring(0, stringTruncateLength.get)
          }
        } else {
          rawString
        }
      case _: Binary =>
        throw new UnsupportedOperationException(
          s"max/min stats is not supported for binary other than string: ${columnMetadata.getPath}")
      case date: Integer if logicalType.isInstanceOf[DateLogicalTypeAnnotation] =>
        DateTimeUtils.toJavaDate(date).toString
      case other => other
    }
  }

  /** The aggregation function used to count null */
  private def aggNullCount(blocks: Seq[BlockMetaData], index: Int): Any = {
    var count = 0L
    blocks.foreach { block =>
      val column = block.getColumns.get(index)
      val statistics = column.getStatistics
      if (!statistics.isNumNullsSet) {
        throw DeltaErrors.deltaStatsCollectionColumnNotFound("nullCount", column.getPath.toString)
      }
      count += statistics.getNumNulls
    }
    count.asInstanceOf[Any]
  }
}

object StatsCollector {
  def apply(
      columnMappingMode: DeltaColumnMappingMode,
      dataSchema: StructType,
      statsSchema: StructType,
      ignoreMissingStats: Boolean = true,
      stringTruncateLength: Option[Int] = None): StatsCollector = {
    columnMappingMode match {
      case NoMapping | NameMapping =>
        StatsCollectorNameMapping(
          dataSchema, statsSchema, ignoreMissingStats, stringTruncateLength)
      case IdMapping =>
        StatsCollectorIdMapping(
          dataSchema, statsSchema, ignoreMissingStats, stringTruncateLength)
      case _ =>
        throw new UnsupportedOperationException(
          s"$columnMappingMode mapping is currently not supported")
    }
  }

  private case class StatsCollectorNameMapping(
      dataSchema: StructType,
      statsSchema: StructType,
      ignoreMissingStats: Boolean,
      stringTruncateLength: Option[Int])
    extends StatsCollector(dataSchema, statsSchema, ignoreMissingStats, stringTruncateLength) {

    /**
     * Maps schema physical field path to parquet metadata column index via parquet metadata column
     * path in NoMapping and NameMapping modes
     */
    override def getSchemaPhysicalPathToParquetIndex(
        blockMetaData: BlockMetaData): Map[Seq[String], Int] = {
      val parquetColumnPathToIndex = getParquetColumnPathToIndex(blockMetaData)
      columnPathSchemaToParquet.collect {
        // Collect mapping of fields in physical schema that actually exist in parquet metadata,
        // parquet metadata can miss field due to schema evolution. In case stats collection is
        // requested on a column that is missing from parquet metadata, we will catch this in
        // collectStats when looking up in this map.
        case (schemaPath, parquetPath) if parquetColumnPathToIndex.contains(parquetPath) =>
          schemaPath -> parquetColumnPathToIndex(parquetPath)
      }
    }

    /**
     * A map from schema field path (with physical names) to parquet metadata column path of schema
     * leaf fields with special handling of Array and Map.
     *
     * Here is an example:
     *
     * Data Schema (physical name in the parenthesis)
     * | -- id (a4def3): INT
     * | -- history (23aa42): STRUCT
     *     | -- cost (23ddb0): DOUBLE
     *     | -- events (23dda1): ARRAY[STRING]
     * | -- info (abb4d2): MAP[STRING, STRING]
     *
     * Block Metadata:
     * Columns: [ [a4def3], [23aa42, 23ddb0], [23ddb0, 23dda1, list, element],
     *            [abb4d2, key_value, key], [abb4d2, key_value, value] ]
     *
     * The mapping:
     *   [a4def3] -> [a4def3]
     *   [23aa42, 23ddb0] -> [23aa42, 23ddb0]
     *   [23ddb0, 23dda1] -> [23ddb0, 23dda1, list, element]
     *   [abb4d2] -> [abb4d2, key_value, key]
     */
    private lazy val columnPathSchemaToParquet: Map[Seq[String], Seq[String]] = {
      // Parquet metadata column path contains addition keywords for Array and Map. Here we only
      // support 2 cases below since stats is not available in the other cases:
      // 1. Array with non-null elements of primitive types
      // 2. Map with key of primitive types
      schemaPhysicalPathAndSchemaField.map {
        case(path, field) =>
          field.dataType match {
            // Here we don't check array element type and map key type for primitive type since
            // parquet metadata column path always points to a primitive column. In other words,
            // the type is primitive if the column path can be found in parquet metadata later.
            case ArrayType(_, false) => path -> (path ++ Seq("list", "element"))
            case MapType(_, _, _) => path -> (path ++ Seq("key_value", "key"))
            case _ => path -> path
          }
      }.toMap
    }

    /**
     * Returns a map from parquet metadata column path to index.
     *
     * Here is an example:
     *
     * Data Schema:
     * |-- id : INT
     * |-- person : STRUCT
     *     |-- name: STRING
     *     |-- phone: INT
     * |-- eligible: BOOLEAN
     *
     * Block Metadata:
     * Columns: [ [id], [person, name], [person, phone], [eligible] ]
     *
     * The mapping:
     *   [id] -> 0
     *   [person, name] -> 1
     *   [person, phone] -> 2
     *   [eligible] -> 3
     */
    private def getParquetColumnPathToIndex(block: BlockMetaData): Map[Seq[String], Int] = {
      block.getColumns.asScala.zipWithIndex.map {
        case (column, i) => column.getPath.toArray.toSeq -> i
      }.toMap
    }
  }

  private case class StatsCollectorIdMapping(
      dataSchema: StructType,
      statsSchema: StructType,
      ignoreMissingStats: Boolean,
      stringTruncateLength: Option[Int])
    extends StatsCollector(dataSchema, statsSchema, ignoreMissingStats, stringTruncateLength) {

    // Define a FieldId type to better disambiguate between ids and indices in the code
    type FieldId = Int

    /**
     * Maps schema physical field path to parquet metadata column index via parquet metadata column
     * id in IdMapping mode.
     */
    override def getSchemaPhysicalPathToParquetIndex(
        blockMetaData: BlockMetaData): Map[Seq[String], Int] = {
      val parquetColumnIdToIndex = getParquetColumnIdToIndex(blockMetaData)
      schemaPhysicalPathToColumnId.collect {
        // Collect mapping of fields in physical schema that actually exist in parquet metadata,
        // parquet metadata can miss field due to schema evolution and non-primitive types like Map
        // and Array. In case stats collection is requested on a column that is missing from
        // parquet metadata, we will catch this in collectStats when looking up in this map.
        case (schemaPath, columnId) if parquetColumnIdToIndex.contains(columnId) =>
          schemaPath -> parquetColumnIdToIndex(columnId)
      }
    }

    /**
     * A map from schema field path (with physical names) to parquet metadata column id of schema
     * leaf fields.
     *
     * Here is an example:
     *
     * Data Schema (physical name, id in the parenthesis)
     * | -- id (a4def3, 1): INT
     * | -- history (23aa42, 2): STRUCT
     *     | -- cost (23ddb0, 3): DOUBLE
     *     | -- events (23dda1, 4): ARRAY[STRING]
     * | -- info (abb4d2, 5): MAP[STRING, STRING]
     *
     * The mapping:
     *   [a4def3] -> 1
     *   [23aa42, 23ddb0] -> 3
     *   [23ddb0, 23dda1] -> 4
     *   [abb4d2] -> 5
     */
    private lazy val schemaPhysicalPathToColumnId: Map[Seq[String], FieldId] = {
      schemaPhysicalPathAndSchemaField.map {
        case (path, field) => path -> DeltaColumnMapping.getColumnId(field)
      }.toMap
    }

    /**
     * Returns a map from parquet metadata column id to column index by skipping columns without id.
     * E.g., subfields of ARRAY and MAP don't have id assigned.
     *
     * Here is an example:
     *
     * Data Schema (id in the parenthesis):
     * |-- id (1) : INT
     * |-- person (2) : STRUCT
     *     |-- names (3) : ARRAY[STRING]
     *     |-- phones (4) : MAP[STRING, INT]
     * |-- eligible (5) : BOOLEAN
     *
     * Block Metadata (id in the parenthesis):
     * Columns: [ [id](1), [person, names, list, element](null),
     *            [person, phones, key_value, key](null), [person, phones, key_value, value](null),
     *            [eligible](5) ]
     *
     * The mapping: 1 -> 0, 5 -> 4
     */
    private def getParquetColumnIdToIndex(block: BlockMetaData): Map[FieldId, Int] = {
      block.getColumns.asScala.zipWithIndex.collect {
        // Id of parquet metadata column is not guaranteed, subfields of Map and Array don't have
        // id assigned. In case id is missing and null, we skip the parquet metadata column here
        // and will catch this in collectStats when looking up in this map.
        case (column, i) if column.getPrimitiveType.getId != null =>
          column.getPrimitiveType.getId.intValue() -> i
      }.toMap
    }
  }
}
