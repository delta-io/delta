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

import java.util.Locale

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import org.apache.spark.sql.delta.{Checkpoints, DeletionVectorsTableFeature, DeltaColumnMapping, DeltaColumnMappingMode, DeltaConfigs, DeltaErrors, DeltaIllegalArgumentException, DeltaLog, DeltaUDF, NoMapping}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY
import org.apache.spark.sql.delta.DeltaOperations.ComputeStats
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.delta.schema.SchemaUtils.transformSchema
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaStatistics._
import org.apache.spark.sql.delta.stats.StatisticsCollection.getIndexedColumns
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, AstBuilder, ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.MultipartIdentifierListContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Used to report metrics on how predicates are used to prune the set of
 * files that are read by a query.
 *
 * @param predicate         A user readable version of the predicate.
 * @param pruningType       One of {partition, dataStats, none}.
 * @param filesMissingStats The number of files that were included due to missing statistics.
 * @param filesDropped      The number of files that were dropped by this predicate.
 */
case class QueryPredicateReport(
    predicate: String,
    pruningType: String,
    filesMissingStats: Long,
    filesDropped: Long)

/** Used to report details about prequery filtering of what data is scanned. */
case class FilterMetric(numFiles: Long, predicates: Seq[QueryPredicateReport])

/**
 * A helper trait that constructs expressions that can be used to collect global
 * and column level statistics for a collection of data, given its schema.
 *
 * Global statistics (such as the number of records) are stored as top level columns.
 * Per-column statistics (such as min/max) are stored in a struct that mirrors the
 * schema of the data.
 *
 * To illustrate, here is an example of a data schema along with the schema of the statistics
 * that would be collected.
 *
 * Data Schema:
 *  {{{
 *  |-- a: struct (nullable = true)
 *  |    |-- b: struct (nullable = true)
 *  |    |    |-- c: long (nullable = true)
 *  }}}
 *
 * Collected Statistics:
 *  {{{
 *  |-- stats: struct (nullable = true)
 *  |    |-- numRecords: long (nullable = false)
 *  |    |-- minValues: struct (nullable = false)
 *  |    |    |-- a: struct (nullable = false)
 *  |    |    |    |-- b: struct (nullable = false)
 *  |    |    |    |    |-- c: long (nullable = true)
 *  |    |-- maxValues: struct (nullable = false)
 *  |    |    |-- a: struct (nullable = false)
 *  |    |    |    |-- b: struct (nullable = false)
 *  |    |    |    |    |-- c: long (nullable = true)
 *  |    |-- nullCount: struct (nullable = false)
 *  |    |    |-- a: struct (nullable = false)
 *  |    |    |    |-- b: struct (nullable = false)
 *  |    |    |    |    |-- c: long (nullable = true)
 *  }}}
 */
trait StatisticsCollection extends DeltaLogging {
  protected def spark: SparkSession
  /** The schema of the target table of this statistics collection. */
  def tableSchema: StructType
  /**
   * The output attributes (`outputAttributeSchema`) that are replaced with table schema with
   * the physical mapping information.
   * NOTE: The partition columns' definitions are not included in this schema.
   */
  def outputTableStatsSchema: StructType
  /**
   * The schema of the output attributes of the write queries that needs to collect statistics.
   * The partition columns' definitions are not included in this schema.
   */
  def outputAttributeSchema: StructType
  /** The statistic indexed column specification of the target delta table. */
  val statsColumnSpec: DeltaStatsColumnSpec
  /** The column mapping mode of the target delta table. */
  def columnMappingMode: DeltaColumnMappingMode

  protected def protocol: Protocol

  lazy val deletionVectorsSupported = protocol.isFeatureSupported(DeletionVectorsTableFeature)

  private def effectiveSchema: StructType = if (statsColumnSpec.numIndexedColsOpt.isDefined) {
    outputTableStatsSchema
  } else {
    tableSchema
  }

  private lazy val explodedDataSchemaNames: Seq[String] =
    SchemaMergingUtils.explodeNestedFieldNames(outputAttributeSchema)

  /**
   * statCollectionPhysicalSchema is the schema that is composed of all the columns that have the
   * stats collected with our current table configuration.
   */
  lazy val statCollectionPhysicalSchema: StructType =
    getIndexedColumns(explodedDataSchemaNames, statsColumnSpec, effectiveSchema, columnMappingMode)

  /**
   * statCollectionLogicalSchema is the logical schema that is composed of all the columns that have
   * the stats collected with our current table configuration.
   */
  lazy val statCollectionLogicalSchema: StructType =
    getIndexedColumns(explodedDataSchemaNames, statsColumnSpec, effectiveSchema, NoMapping)

  /**
   * Traverses the [[statisticsSchema]] for the provided [[statisticsColumn]]
   * and applies [[function]] to leaves.
   *
   * Note, for values that are outside the domain of the partial function we keep the original
   * column. If the caller wants to drop the column needs to explicitly return None.
   */
  def applyFuncToStatisticsColumn(
      statisticsSchema: StructType,
      statisticsColumn: Column)(
      function: PartialFunction[(Column, StructField), Option[Column]]): Seq[Column] = {
    statisticsSchema.flatMap {
      case StructField(name, s: StructType, _, _) =>
        val column = statisticsColumn.getItem(name)
        applyFuncToStatisticsColumn(s, column)(function) match {
          case colSeq if colSeq.nonEmpty => Some(struct(colSeq: _*) as name)
          case _ => None
        }

      case structField@StructField(name, _, _, _) =>
        val column = statisticsColumn.getItem(name)
        function.lift(column, structField).getOrElse(Some(column)).map(_.as(name))
    }
  }

  /**
   * Sets the TIGHT_BOUNDS column to false and converts the logical nullCount
   * to a tri-state nullCount. The nullCount states are the following:
   *    1) For "all-nulls" columns we set the physical nullCount which is equal to the
   *       physical numRecords.
   *    2) "no-nulls" columns remain unchanged, i.e. zero nullCount is the same for both
   *       physical and logical representations.
   *    3) For "some-nulls" columns, we leave the existing value. In files with wide bounds,
   *       the nullCount in SOME_NULLs columns is considered unknown.
   *
   * The file's state can transition back to tight when statistics are recomputed. In that case,
   * TIGHT_BOUNDS is set back to true and nullCount back to the logical value.
   *
   * Note, this function gets as input parsed statistics and returns a json document
   * similarly to allFiles. To further match the behavior of allFiles we always return
   * a column named `stats` instead of statsColName.
   *
   * @param withStats A dataFrame of actions with parsed statistics.
   * @param statsColName The name of the parsed statistics column.
   */
  def updateStatsToWideBounds(withStats: DataFrame, statsColName: String): DataFrame = {
    val dvCardinalityCol = coalesce(col("deletionVector.cardinality"), lit(0))
    val physicalNumRecordsCol = col(s"$statsColName.$NUM_RECORDS")
    val logicalNumRecordsCol = physicalNumRecordsCol - dvCardinalityCol
    val nullCountCol = col(s"$statsColName.$NULL_COUNT")
    val tightBoundsCol = col(s"$statsColName.$TIGHT_BOUNDS")
    val statsSchema = withStats.schema.apply(statsColName).dataType.asInstanceOf[StructType]

    val allStatCols = ALL_STAT_FIELDS.flatMap {
      case TIGHT_BOUNDS => Some(lit(false).as(TIGHT_BOUNDS))
      case NULL_COUNT if statsSchema.names.contains(NULL_COUNT) =>
        // Use the schema of the existing stats column. We only want to modify the existing
        // nullCount stats. Note, when the column mapping mode is enabled, the schema uses
        // the physical column names, not the logical names.
        val nullCountSchema = statsSchema
          .apply(NULL_COUNT).dataType.asInstanceOf[StructType]

        // When bounds are tight and we are about to transition to wide, store the physical null
        // count for ALL_NULLs columns.
        val nullCountColSeq = applyFuncToStatisticsColumn(nullCountSchema, nullCountCol) {
          case (c, _) =>
            val allNullTightBounds = tightBoundsCol && (c === logicalNumRecordsCol)
            Some(when(allNullTightBounds, physicalNumRecordsCol).otherwise(c))
        }
        Some(struct(nullCountColSeq: _*).as(NULL_COUNT))
      case f if statsSchema.names.contains(f) => Some(col(s"${statsColName}.${f}"))
      case _ =>
        // This stat is not present in the original stats schema, so we should not include it.
        None
    }

    // This may be very expensive because it is rewriting JSON.
    withStats
      .withColumn("stats", when(col(statsColName).isNotNull, to_json(struct(allStatCols: _*))))
      .drop(col(Checkpoints.STRUCT_STATS_COL_NAME)) // Note: does not always exist.
  }

  /**
   * Returns a struct column that can be used to collect statistics for the current
   * schema of the table.
   * The types we keep stats on must be consistent with DataSkippingReader.SkippingEligibleLiteral.
   * If a column is missing from dataSchema (which will be filled with nulls), we will only
   * collect the NULL_COUNT stats for it as the number of rows.
   */
  lazy val statsCollector: Column = {
    val stringPrefix =
      spark.sessionState.conf.getConf(DeltaSQLConf.DATA_SKIPPING_STRING_PREFIX_LENGTH)

    // On file initialization/stat recomputation TIGHT_BOUNDS is always set to true
    val tightBoundsColOpt =
      Option.when(deletionVectorsSupported &&
          !spark.sessionState.conf.getConf(DeltaSQLConf.TIGHT_BOUND_COLUMN_ON_FILE_INIT_DISABLED)) {
        lit(true).as(TIGHT_BOUNDS)
      }

    val statCols = Seq(
      count(new Column("*")) as NUM_RECORDS,
      collectStats(MIN, statCollectionPhysicalSchema) {
        // Truncate string min values as necessary
        case (c, SkippingEligibleDataType(StringType), true) =>
          substring(min(c), 0, stringPrefix)

        // Collect all numeric min values
        case (c, SkippingEligibleDataType(_), true) =>
          min(c)
      },
      collectStats(MAX, statCollectionPhysicalSchema) {
        // Truncate and pad string max values as necessary
        case (c, SkippingEligibleDataType(StringType), true) =>
          val udfTruncateMax =
            DeltaUDF.stringFromString(StatisticsCollection.truncateMaxStringAgg(stringPrefix)_)
          udfTruncateMax(max(c))

        // Collect all numeric max values
        case (c, SkippingEligibleDataType(_), true) =>
          max(c)
      },
      collectStats(NULL_COUNT, statCollectionPhysicalSchema) {
        case (c, _, true) => sum(when(c.isNull, 1).otherwise(0))
        case (_, _, false) => count(new Column("*"))
      }) ++ tightBoundsColOpt

    struct(statCols: _*).as("stats")
  }


  /** Returns schema of the statistics collected. */
  lazy val statsSchema: StructType = {
    // In order to get the Delta min/max stats schema from table schema, we do 1) replace field
    // name with physical name 2) set nullable to true 3) only keep stats eligible fields
    // 4) omits metadata in table schema as Delta stats schema does not need the metadata
    def getMinMaxStatsSchema(schema: StructType): Option[StructType] = {
      val fields = schema.fields.flatMap {
        case f@StructField(_, dataType: StructType, _, _) =>
          getMinMaxStatsSchema(dataType).map { newDataType =>
            StructField(DeltaColumnMapping.getPhysicalName(f), newDataType)
          }
        case f@StructField(_, SkippingEligibleDataType(dataType), _, _) =>
          Some(StructField(DeltaColumnMapping.getPhysicalName(f), dataType))
        case _ => None
      }
      if (fields.nonEmpty) Some(StructType(fields)) else None
    }

    // In order to get the Delta null count schema from table schema, we do 1) replace field name
    // with physical name 2) set nullable to true 3) use LongType for all fields
    // 4) omits metadata in table schema as Delta stats schema does not need the metadata
    def getNullCountSchema(schema: StructType): Option[StructType] = {
      val fields = schema.fields.flatMap {
        case f@StructField(_, dataType: StructType, _, _) =>
          getNullCountSchema(dataType).map { newDataType =>
            StructField(DeltaColumnMapping.getPhysicalName(f), newDataType)
          }
        case f: StructField =>
          Some(StructField(DeltaColumnMapping.getPhysicalName(f), LongType))
      }
      if (fields.nonEmpty) Some(StructType(fields)) else None
    }

    val minMaxStatsSchemaOpt = getMinMaxStatsSchema(statCollectionPhysicalSchema)
    val nullCountSchemaOpt = getNullCountSchema(statCollectionPhysicalSchema)
    val tightBoundsFieldOpt =
      Option.when(deletionVectorsSupported)(TIGHT_BOUNDS -> BooleanType)

    val fields =
      Array(NUM_RECORDS -> LongType) ++
      minMaxStatsSchemaOpt.map(MIN -> _) ++
      minMaxStatsSchemaOpt.map(MAX -> _) ++
      nullCountSchemaOpt.map(NULL_COUNT -> _) ++
      tightBoundsFieldOpt

    StructType(fields.map {
      case (name, dataType) => StructField(name, dataType)
    })
  }

  /**
   * Recursively walks the given schema, constructing an expression to calculate
   * multiple statistics that mirrors structure of the data. When `function` is
   * defined for a given column, it return value is added to statistics structure.
   * When `function` is not defined, that column is skipped.
   *
   * @param name     The name of the top level column for this statistic (i.e. minValues).
   * @param schema   The schema of the data to collect statistics from.
   * @param function A partial function that is passed a tuple of (column, metadata about that
   *                 column, a flag that indicates whether the column is in the data schema). Based
   *                 on the metadata and flag, the function can decide if the given statistic should
   *                 be collected on the column by returning the correct aggregate expression.
   * @param includeAllColumns  should statistics all the columns be included?
   */
  private def collectStats(
      name: String,
      schema: StructType,
      includeAllColumns: Boolean = false)(
      function: PartialFunction[(Column, StructField, Boolean), Column]): Column = {

    def collectStats(
      schema: StructType,
      parent: Option[Column],
      parentFields: Seq[String],
      function: PartialFunction[(Column, StructField, Boolean), Column]): Seq[Column] = {
      schema.flatMap {
        case f @ StructField(name, s: StructType, _, _) =>
          val column = parent.map(_.getItem(name))
            .getOrElse(Column(UnresolvedAttribute.quoted(name)))
          val stats = collectStats(s, Some(column), parentFields :+ name, function)
          if (stats.nonEmpty) {
            Some(struct(stats: _*) as DeltaColumnMapping.getPhysicalName(f))
          } else {
            None
          }
        case f @ StructField(name, _, _, _) =>
          val fieldPath = UnresolvedAttribute(parentFields :+ name).name
          val column = parent.map(_.getItem(name))
            .getOrElse(Column(UnresolvedAttribute.quoted(name)))
          // alias the column with its physical name
          // Note: explodedDataSchemaNames comes from dataSchema. In the read path, dataSchema comes
          // from the table's metadata.dataSchema, which is the same as tableSchema. In the
          // write path, dataSchema comes from the DataFrame schema. We then assume
          // TransactionWrite.writeFiles has normalized dataSchema, and
          // TransactionWrite.getStatsSchema has done the column mapping for tableSchema and
          // dropped the partition columns for both dataSchema and tableSchema.
          function.lift((column, f, explodedDataSchemaNames.contains(fieldPath))).
            map(_.as(DeltaColumnMapping.getPhysicalName(f)))
      }
    }

    val stats = collectStats(schema, None, Nil, function)
    if (stats.nonEmpty) {
      struct(stats: _*).as(name)
    } else {
      lit(null).as(name)
    }
  }
}

/**
 * Specifies the set of columns to be used for stats collection on a table.
 * The `deltaStatsColumnNamesOpt` has higher priority than `numIndexedColsOpt`. Thus, if
 * `deltaStatsColumnNamesOpt` is not None, StatisticsCollection would only collects file statistics
 * for all columns inside it. Otherwise, `numIndexedColsOpt` is used.
 */
case class DeltaStatsColumnSpec(
    deltaStatsColumnNamesOpt: Option[Seq[UnresolvedAttribute]],
    numIndexedColsOpt: Option[Int]) {
  require(deltaStatsColumnNamesOpt.isEmpty || numIndexedColsOpt.isEmpty)
}

object StatisticsCollection extends DeltaCommand {

  val ASCII_MAX_CHARACTER = '\u007F'

  val UTF8_MAX_CHARACTER = new String(Character.toChars(Character.MAX_CODE_POINT))

  /**
   * The SQL grammar already includes a `multipartIdentifierList` rule for parsing a string into a
   * list of multi-part identifiers. We just expose it here, with a custom parser and AstBuilder.
   */
  private class SqlParser extends AbstractSqlParser {
    override val astBuilder = new AstBuilder {
      override def visitMultipartIdentifierList(ctx: MultipartIdentifierListContext)
      : Seq[UnresolvedAttribute] = ParserUtils.withOrigin(ctx) {
        ctx.multipartIdentifier.asScala.toSeq.map(typedVisit[Seq[String]])
          .map(new UnresolvedAttribute(_))
      }
    }
    def parseMultipartIdentifierList(sqlText: String): Seq[UnresolvedAttribute] = {
      parse(sqlText) { parser =>
        astBuilder.visitMultipartIdentifierList(parser.multipartIdentifierList())
      }
    }
  }
  private val parser = new SqlParser

  /** Parses a comma-separated list of column names; returns None if parsing fails. */
  def parseDeltaStatsColumnNames(deltaStatsColNames: String): Option[Seq[UnresolvedAttribute]] = {
    // The parser rejects empty lists, so handle that specially here.
    if (deltaStatsColNames.trim.isEmpty) return Some(Nil)
    try {
      Some(parser.parseMultipartIdentifierList(deltaStatsColNames))
    } catch {
      case _: ParseException => None
    }
  }

  /**
   * This method is the wrapper method to validates the DATA_SKIPPING_STATS_COLUMNS value of
   * metadata.
   */
  def validateDeltaStatsColumns(metadata: Metadata): Unit = {
    DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.fromMetaData(metadata).foreach { statsColumns =>
      StatisticsCollection.validateDeltaStatsColumns(
        metadata.dataSchema, metadata.partitionColumns, statsColumns
      )
    }
  }

  /**
   * This method validates that the data type of data skipping column supports data skipping
   * based on file statistics.
   * @param name The name of the data skipping column for validating data type.
   * @param dataType The data type of the data skipping column.
   * @param columnPaths The column paths of all valid fields.
   */
  private def validateDataSkippingType(
      name: String,
      dataType: DataType,
      columnPaths: ArrayBuffer[String]): Unit = dataType match {
    case s: StructType =>
      s.foreach { field =>
        validateDataSkippingType(name + "." + field.name, field.dataType, columnPaths)
      }
    case SkippingEligibleDataType(_) => columnPaths.append(name)
    case _ =>
      throw new DeltaIllegalArgumentException(
        errorClass = "DELTA_COLUMN_DATA_SKIPPING_NOT_SUPPORTED_TYPE",
        messageParameters = Array(name, dataType.toString))
  }

  /**
   * This method validates whether the DATA_SKIPPING_STATS_COLUMNS value satisfies following
   * conditions:
   * 1. Delta statistics columns must not be partitioned column.
   * 2. Delta statistics column must exist in delta table's schema.
   * 3. Delta statistics columns must be data skipping type.
   */
  def validateDeltaStatsColumns(
      schema: StructType, partitionColumns: Seq[String], deltaStatsColumnsConfigs: String): Unit = {
    val partitionColumnSet = partitionColumns.map(_.toLowerCase(Locale.ROOT)).toSet
    val visitedColumns = ArrayBuffer.empty[String]
    parseDeltaStatsColumnNames(deltaStatsColumnsConfigs).foreach { columns =>
      columns.foreach { columnAttribute =>
        val columnFullPath = columnAttribute.nameParts
        // Delta statistics columns must not be partitioned column.
        if (partitionColumnSet.contains(columnAttribute.name.toLowerCase(Locale.ROOT))) {
          throw new DeltaIllegalArgumentException(
            errorClass = "DELTA_COLUMN_DATA_SKIPPING_NOT_SUPPORTED_PARTITIONED_COLUMN",
            messageParameters = Array(columnAttribute.name))
        }
        // Delta statistics column must exist in delta table's schema.
        SchemaUtils.findColumnPosition(columnFullPath, schema)
        // Delta statistics columns must be data skipping type.
        val (prefixPath, columnName) = columnFullPath.splitAt(columnFullPath.size - 1)
        transformSchema(schema, Some(columnName.head)) {
          case (`prefixPath`, struct @ StructType(_), _) =>
            val columnField = struct(columnName.head)
            validateDataSkippingType(columnAttribute.name, columnField.dataType, visitedColumns)
            struct
          case (_, other, _) => other
        }
      }
    }
    val duplicatedColumnNames = visitedColumns
      .groupBy(identity)
      .collect { case (attribute, occurrences) if occurrences.size > 1 => attribute }
      .toSeq
    if (duplicatedColumnNames.size > 0) {
      throw new DeltaIllegalArgumentException(
        errorClass = "DELTA_DUPLICATE_DATA_SKIPPING_COLUMNS",
        messageParameters = Array(duplicatedColumnNames.mkString(","))
      )
    }
  }

  /**
   * Removes the dropped columns from delta statistics column list inside
   * DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.
   * Note: This method is matching the logical name of tables with the columns inside
   * DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.
   */
  def dropDeltaStatsColumns(
      metadata: Metadata,
      columnsToDrop: Seq[Seq[String]]): Map[String, String] = {
    if (columnsToDrop.isEmpty) return Map.empty[String, String]
    val deltaStatsColumnSpec = configuredDeltaStatsColumnSpec(metadata)
    deltaStatsColumnSpec.deltaStatsColumnNamesOpt.map { deltaColumnsNames =>
      val droppedColumnSet = columnsToDrop.toSet
      val deltaStatsColumnStr = deltaColumnsNames
        .map(_.nameParts)
        .filterNot { attributeNameParts =>
          droppedColumnSet.filter { droppedColumnParts =>
            val commonPrefix = droppedColumnParts.zip(attributeNameParts)
              .takeWhile { case (left, right) => left == right }
              .size
            commonPrefix == droppedColumnParts.size
          }.nonEmpty
        }
        .map(columnParts => UnresolvedAttribute(columnParts).name)
        .mkString(",")
      Map(DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.key -> deltaStatsColumnStr)
    }.getOrElse(Map.empty[String, String])
  }

  /**
   * Rename the delta statistics column `oldColumnPath` of DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS
   * to `newColumnPath`.
   * Note: This method is matching the logical name of tables with the columns inside
   * DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.
   */
  def renameDeltaStatsColumn(
      metadata: Metadata,
      oldColumnPath: Seq[String],
      newColumnPath: Seq[String]): Map[String, String] = {
    if (oldColumnPath == newColumnPath) return Map.empty[String, String]
    val deltaStatsColumnSpec = configuredDeltaStatsColumnSpec(metadata)
    deltaStatsColumnSpec.deltaStatsColumnNamesOpt.map { deltaColumnsNames =>
      val deltaStatsColumnsPath = deltaColumnsNames
        .map(_.nameParts)
        .map { attributeNameParts =>
          val commonPrefix = oldColumnPath.zip(attributeNameParts)
            .takeWhile { case (left, right) => left == right }
            .size
          if (commonPrefix == oldColumnPath.size) {
            newColumnPath ++ attributeNameParts.takeRight(attributeNameParts.size - commonPrefix)
          } else {
            attributeNameParts
          }
        }
        .map(columnParts => UnresolvedAttribute(columnParts).name)
      Map(
        DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.key -> deltaStatsColumnsPath.mkString(",")
      )
    }.getOrElse(Map.empty[String, String])
  }

  /** Returns the configured set of columns to be used for stats collection on a table */
  def configuredDeltaStatsColumnSpec(metadata: Metadata): DeltaStatsColumnSpec = {
    val indexedColNamesOpt = DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.fromMetaData(metadata)
    val numIndexedCols = DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(metadata)
    indexedColNamesOpt.map { indexedColNames =>
      DeltaStatsColumnSpec(parseDeltaStatsColumnNames(indexedColNames), None)
    }.getOrElse {
      DeltaStatsColumnSpec(None, Some(numIndexedCols))
    }
  }

  /**
   * Convert the logical name of each field to physical name according to the column mapping mode.
   */
  private def convertToPhysicalName(
      fullPath: String,
      field: StructField,
      schemaNames: Seq[String],
      mappingMode: DeltaColumnMappingMode): StructField = {
    // If mapping mode is NoMapping or the dataSchemaName already contains the mapped
    // column name, the schema mapping can be skipped.
    if (mappingMode == NoMapping || schemaNames.contains(fullPath)) return field
    // Get the physical co
    val physicalName = field.metadata.getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
    field.dataType match {
      case structType: StructType =>
        val newDataType = StructType(
          structType.map(child => convertToPhysicalName(fullPath, child, schemaNames, mappingMode))
        )
        field.copy(name = physicalName, dataType = newDataType)
      case _ => field.copy(name = physicalName)
    }
  }

  /**
   * Generates a filtered data schema for stats collection.
   * Note: This method is matching the logical name of tables with the columns inside
   * DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS. The output of the filter schema is translated into
   * physical name.
   *
   * @param schemaNames the full name path of all columns inside `schema`.
   * @param schema the original data schema.
   * @param statsColPaths the specific set of columns to collect stats on.
   * @param mappingMode the column mapping mode of this statistics collection.
   * @param parentPath the parent column path of `schema`.
   * @return filtered schema
   */
  private def filterSchema(
      schemaNames: Seq[String],
      schema: StructType,
      statsColPaths: Seq[Seq[String]],
      mappingMode: DeltaColumnMappingMode,
      parentPath: Seq[String] = Seq.empty): StructType = {
    // Find the unique column names at this nesting depth, each with its path remainders (if any)
    val cols = statsColPaths.groupBy(_.head).mapValues(_.map(_.tail))
    val newSchema = schema.flatMap { field =>
      val lowerCaseFieldName = field.name.toLowerCase(Locale.ROOT)
      cols.get(lowerCaseFieldName).flatMap { paths =>
        field.dataType match {
          case _ if paths.forall(_.isEmpty) =>
            // Convert full path to lower cases to avoid schema name contains upper case
            // characters.
            val fullPath = (parentPath :+ field.name).mkString(".").toLowerCase(Locale.ROOT)
            Some(convertToPhysicalName(fullPath, field, schemaNames, mappingMode))
          case fieldSchema: StructType =>
            // Convert full path to lower cases to avoid schema name contains upper case
            // characters.
            val fullPath = (parentPath :+ field.name).mkString(".").toLowerCase(Locale.ROOT)
            val physicalName = if (mappingMode == NoMapping || schemaNames.contains(fullPath)) {
              field.name
            } else {
              field.metadata.getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
            }
            // Recurse into the child fields of this struct.
            val newSchema = filterSchema(
              schemaNames,
              fieldSchema,
              paths.filterNot(_.isEmpty),
              mappingMode,
              parentPath:+ field.name
            )
            Some(field.copy(name = physicalName, dataType = newSchema))
          case _ =>
            // Filter expected a nested field and this isn't nested. No match
            None
        }
      }
    }
    StructType(newSchema.toArray)
  }

  /**
   * Computes the set of columns to be used for stats collection on a table. Specific named columns
   * take precedence, if provided; otherwise the first numIndexedColsOpt are extracted from the
   * schema.
   */
  def getIndexedColumns(
      schemaNames: Seq[String],
      spec: DeltaStatsColumnSpec,
      schema: StructType,
      mappingMode: DeltaColumnMappingMode): StructType = {
    spec.deltaStatsColumnNamesOpt
      .map { indexedColNames =>
        // convert all index columns to lower case characters to avoid user assigning any upper
        // case characters.
        val indexedColPaths = indexedColNames.map(_.nameParts.map(_.toLowerCase(Locale.ROOT)))
        filterSchema(schemaNames, schema, indexedColPaths, mappingMode)
      }
      .getOrElse {
        val numIndexedCols = spec.numIndexedColsOpt.get
        if (numIndexedCols < 0) {
          schema // negative means don't truncate the schema
        } else {
          truncateSchema(schema, numIndexedCols)._1
        }
      }
  }

  /**
   * Generates a truncated data schema for stats collection.
   * @param schema the original data schema
   * @param indexedCols the maximum number of leaf columns to collect stats on
   * @return truncated schema and the number of leaf columns in this schema
   */
  private def truncateSchema(schema: StructType, indexedCols: Int): (StructType, Int) = {
    var accCnt = 0
    var i = 0
    val fields = new ArrayBuffer[StructField]()
    while (i < schema.length && accCnt < indexedCols) {
      val field = schema.fields(i)
      val newField = field match {
        case StructField(name, st: StructType, nullable, metadata) =>
          val (newSt, cnt) = truncateSchema(st, indexedCols - accCnt)
          accCnt += cnt
          StructField(name, newSt, nullable, metadata)
        case f =>
          accCnt += 1
          f
      }
      i += 1
      fields += newField
    }
    (StructType(fields.toSeq), accCnt)
  }

  /**
   * Compute the AddFile entries with delta statistics entries by aggregating the data skipping
   * columns of each parquet file.
   */
  private def computeNewAddFiles(
      deltaLog: DeltaLog,
      txn: OptimisticTransaction,
      files: Seq[AddFile]): Array[AddFile] = {
    val dataPath = deltaLog.dataPath
    val pathToAddFileMap = generateCandidateFileMap(dataPath, files)
    val persistentDVsReadable = DeletionVectorUtils.deletionVectorsReadable(txn.snapshot)
    // Throw error when the table contains DVs, because existing method of stats
    // recomputation doesn't work on tables with DVs. It needs to take into consideration of
    // DV files (TODO).
    if (persistentDVsReadable) {
      throw DeltaErrors.statsRecomputeNotSupportedOnDvTables()
    }
    val fileDataFrame = deltaLog
      .createDataFrame(txn.snapshot, addFiles = files, isStreaming = false)
      .withColumn("path", col("_metadata.file_path"))
    val newStats = fileDataFrame.groupBy(col("path")).agg(to_json(txn.statsCollector))
    newStats.collect().map { r =>
      val add = getTouchedFile(dataPath, r.getString(0), pathToAddFileMap)
      add.copy(dataChange = false, stats = r.getString(1))
    }
  }

  /**
   * Recomputes statistics for a Delta table. This can be used to compute stats if they were never
   * collected or to recompute corrupted statistics.
   * @param deltaLog Delta log for the table to update.
   * @param predicates Which subset of the data to recompute stats for. Predicates must use only
   *                   partition columns.
   * @param fileFilter Filter for which AddFiles to recompute stats for.
   */
  def recompute(
      spark: SparkSession,
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable],
      predicates: Seq[Expression] = Seq(Literal(true)),
      fileFilter: AddFile => Boolean = af => true): Unit = {
    val txn = deltaLog.startTransaction(catalogTable)
    verifyPartitionPredicates(spark, txn.metadata.partitionColumns, predicates)
    // Save the current AddFiles that match the predicates so we can update their stats
    val files = txn.filterFiles(predicates).filter(fileFilter)
    val newAddFiles = computeNewAddFiles(deltaLog, txn, files)
    txn.commit(newAddFiles, ComputeStats(predicates))
  }

  def truncateMinStringAgg(prefixLen: Int)(input: String): String = {
    if (input == null || input.length <= prefixLen) {
      return input
    }
    if (prefixLen <= 0) {
      return null
    }
    if (Character.isHighSurrogate(input.charAt(prefixLen - 1)) &&
        Character.isLowSurrogate(input.charAt(prefixLen))) {
      // If the character at prefixLen - 1 is a high surrogate and the next character is a low
      // surrogate, we need to include the next character in the prefix to ensure that we don't
      // truncate the string in the middle of a surrogate pair.
      input.take(prefixLen + 1)
    } else {
      input.take(prefixLen)
    }
  }

  /**
   * Helper method to truncate the input string `input` to the given `prefixLen` length, while also
   * ensuring the any value in this column is less than or equal to the truncated max in UTF-8
   * encoding.
   */
  def truncateMaxStringAgg(prefixLen: Int)(originalMax: String): String = {
    // scalastyle:off nonascii
    if (originalMax == null || originalMax.length <= prefixLen) {
      return originalMax
    }
    if (prefixLen <= 0) {
      return null
    }

    // Grab the prefix. We want to append max Unicode code point `\uDBFF\uDFFF` as a tie-breaker,
    // but that is only safe if the character we truncated was smaller in UTF-8 encoded binary
    // comparison. Keep extending the prefix until that condition holds, or we run off the end of
    // the string.
    // We also try to use the ASCII max character `\u007F` as a tie-breaker if possible.
    val maxLen = getExpansionLimit(prefixLen)
    // Start with a valid prefix
    var currLen = truncateMinStringAgg(prefixLen)(originalMax).length
    while (currLen <= maxLen) {
      if (currLen >= originalMax.length) {
        // Return originalMax if we have reached the end of the string
        return originalMax
      } else if (currLen + 1 < originalMax.length &&
          originalMax.substring(currLen, currLen + 2) == UTF8_MAX_CHARACTER) {
        // Skip the UTF-8 max character. It occupies two characters in a Scala string.
        currLen += 2
      } else if (originalMax.charAt(currLen) < ASCII_MAX_CHARACTER) {
        return originalMax.take(currLen) + ASCII_MAX_CHARACTER
      } else {
        return originalMax.take(currLen) + UTF8_MAX_CHARACTER
      }
    }

    // Return null when the input string is too long to truncate.
    null
    // scalastyle:on nonascii
  }

  /**
   * Calculates the upper character limit when constructing a maximum is not possible with only
   * prefixLen chars.
   */
  private def getExpansionLimit(prefixLen: Int): Int = 2 * prefixLen
}
