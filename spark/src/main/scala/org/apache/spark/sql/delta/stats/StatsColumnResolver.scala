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

import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.implicits._
import org.apache.spark.sql.delta.stats.DeltaStatistics.MAX

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal, TimestampAdd}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, StructField, StructType,
  TimestampNTZType, TimestampType}

/**
 * Resolves statistics columns for a Delta table snapshot.
 *
 * Encapsulates the logic for traversing the stats schema and table schema
 * to produce typed [[Column]] expressions that reference specific statistics
 * (MIN, MAX, NULL_COUNT, NUM_RECORDS, etc.) for specific table columns.
 *
 * This class is reusable across V1 and V2 code paths. Callers inject the
 * three schema-level dependencies through the constructor, and then call
 * [[getStatsColumnOpt]] or [[getStatsColumnOrNullLiteral]] as needed.
 *
 * @param baseStatsColumn The root column expression for parsed stats (typically `col("stats")`)
 * @param statsSchema     The schema of the parsed statistics struct
 * @param tableSchema     The logical schema of the user table (used for column-mapping lookups)
 */
private[delta] class StatsColumnResolver(
    baseStatsColumn: Column,
    statsSchema: StructType,
    tableSchema: StructType) {

  /**
   * Returns an expression to access the given statistics for a specific column, or None if that
   * stats column does not exist.
   *
   * @param pathToStatType Path components of one of the fields declared by the `DeltaStatistics`
   *                       object. For statistics of collated strings, this path contains the
   *                       versioned collation identifier. In all other cases the path only has one
   *                       element. The path is in reverse order.
   * @param pathToColumn The components of the nested column name to get stats for. The components
   *                     are in reverse order.
   */
  def getStatsColumnOpt(
      pathToStatType: Seq[String], pathToColumn: Seq[String]): Option[Column] = {

    require(pathToStatType.nonEmpty, "No path to stats type provided.")

    // First validate that pathToStatType is a valid path in the statsSchema. We start at the root
    // of the stats schema and then follow the path. Note that the path is stored in reverse order.
    // If one of the path components does not exist, the foldRight operation returns None.
    val (initialColumn, initialFieldType) = pathToStatType
      .foldRight(Option((baseStatsColumn, statsSchema.asInstanceOf[DataType]))) {
        case (statTypePathComponent: String, Some((column: Column, struct: StructType))) =>
          // Find the field matching the current path component name or return None otherwise.
          struct.fields.collectFirst {
            case StructField(name, dataType: DataType, _, _) if name == statTypePathComponent =>
              (column.getField(statTypePathComponent), dataType)
          }
        case _ => None
      }
      // If the requested stats type doesn't even exist, just return None right away. This can
      // legitimately happen if we have no stats at all, or if column stats are disabled (in which
      // case only the NUM_RECORDS stat type is available).
      .getOrElse { return None }

    // Given a set of path segments in reverse order, e.g. column a.b.c is Seq("c", "b", "a"), we
    // use a foldRight operation to build up the requested stats column, by successively applying
    // each new path step against both the table schema and the stats schema. We can't use the stats
    // schema alone, because the caller-provided path segments use logical column names, while the
    // stats schema requires physical column names. Instead, we must step into the table schema to
    // extract that field's physical column name, and use the result to step into the stats schema.
    //
    // We use a three-tuple to track state. The traversal starts with the base column for the
    // requested stat type, the stats schema for the requested stat type, and the table schema. Each
    // step of the traversal emits the updated column, along with the stats schema and table schema
    // elements corresponding to that column.
    val initialState: Option[(Column, DataType, DataType)] =
      Some((initialColumn, initialFieldType, tableSchema))
    pathToColumn
      .foldRight(initialState) {
        // NOTE: Only match on StructType, because we cannot traverse through other DataTypes.
        case (fieldName, Some((statCol, curStatsSchema: StructType, curTableSchema: StructType))) =>
          // First try to step into the table schema
          val tableFieldOpt = curTableSchema.findNestedFieldIgnoreCase(Seq(fieldName))

          // If that worked, try to step into the stats schema, using its physical name
          val statsFieldOpt = tableFieldOpt
            .map(DeltaColumnMapping.getPhysicalName)
            .filter(physicalFieldName => curStatsSchema.exists(_.name == physicalFieldName))
            .map(curStatsSchema(_))

          // If all that succeeds, return the new stats column and the corresponding data types.
          statsFieldOpt.map(statsField =>
            (statCol.getField(statsField.name), statsField.dataType, tableFieldOpt.get.dataType))

        // Propagate failure if the above match failed (or if already None)
        case _ => None
      }
      // Filter out non-leaf columns -- they lack stats so skipping predicates can't use them.
      .filterNot(_._2.isInstanceOf[StructType])
      .map {
        case (statCol, TimestampType, _) if pathToStatType.head == MAX =>
          // SC-22824: For timestamps, JSON serialization will truncate to milliseconds. This means
          // that we must adjust 1 millisecond upwards for max stats, or we will incorrectly skip
          // records that differ only in microsecond precision. (For example, a file containing only
          // 01:02:03.456789 will be written with min == max == 01:02:03.456, so we must consider it
          // to contain the range from 01:02:03.456 to 01:02:03.457.)
          //
          // There is a longer term task SC-22825 to fix the serialization problem that caused this.
          // But we need the adjustment in any case to correctly read stats written by old versions.
          // TimeAdd is removed in Spark 4.1, using TimestampAdd instead
          Column(Cast(TimestampAdd(
            "MILLISECOND",
            new Literal(1L, LongType),
            statCol.expr), TimestampType))
        case (statCol, TimestampNTZType, _) if pathToStatType.head == MAX =>
          // We also apply the same adjustment of max stats that was applied to Timestamp
          // for TimestampNTZ because these 2 types have the same precision in terms of time.
          // TimeAdd is removed in Spark 4.1, using TimestampAdd instead
          Column(Cast(TimestampAdd(
            "MILLISECOND",
            new Literal(1L, LongType),
            statCol.expr), TimestampNTZType))
        case (statCol, _, _) =>
          statCol
      }
  }

  /** Convenience overload for single element stat type paths. */
  def getStatsColumnOpt(
      statType: String, pathToColumn: Seq[String] = Nil): Option[Column] =
    getStatsColumnOpt(Seq(statType), pathToColumn)

  /**
   * Returns an expression to access the given statistics for a specific column, or a NULL
   * literal expression if that column does not exist.
   */
  def getStatsColumnOrNullLiteral(
      statType: String,
      pathToColumn: Seq[String] = Nil): Column =
    getStatsColumnOpt(Seq(statType), pathToColumn).getOrElse(lit(null))

  /** Overload for convenience working with [[StatsColumn]] helpers. */
  def getStatsColumnOpt(stat: StatsColumn): Option[Column] =
    getStatsColumnOpt(stat.pathToStatType, stat.pathToColumn)

  /** Overload for convenience working with [[StatsColumn]] helpers. */
  def getStatsColumnOrNullLiteral(stat: StatsColumn): Column =
    getStatsColumnOpt(stat.pathToStatType, stat.pathToColumn).getOrElse(lit(null))
}

private[delta] object StatsColumnResolver {
  import DeltaStatistics._

  /**
   * Builds the statistics schema for a given table schema.
   *
   * This is the shared entry point used by both V1 ([[StatisticsCollection.statsSchema]])
   * and V2 callers. V1 passes `statCollectionPhysicalSchema` (already filtered by
   * indexed-column spec); V2 passes the full table schema.
   *
   * @param schema             Schema whose columns need stats (physical names).
   *                           V1: `statCollectionPhysicalSchema`;
   *                           V2: full table schema.
   * @param deletionVectorsSupported Whether to include the TIGHT_BOUNDS field.
   * @return The statistics struct schema (numRecords, minValues, maxValues, nullCount, ...).
   */
  def buildStatsSchema(
      schema: StructType,
      deletionVectorsSupported: Boolean = false): StructType = {

    val minMaxOpt = getMinMaxStatsSchema(schema)
    val nullCountOpt = getNullCountSchema(schema)
    val tightBoundsFieldOpt =
      Option.when(deletionVectorsSupported)(TIGHT_BOUNDS -> BooleanType)

    val fields =
      Array(NUM_RECORDS -> LongType) ++
        minMaxOpt.map(MIN -> _) ++
        minMaxOpt.map(MAX -> _) ++
        nullCountOpt.map(NULL_COUNT -> _) ++
        tightBoundsFieldOpt

    StructType(fields.map { case (name, dt) => StructField(name, dt) })
  }

  /**
   * Computes the min/max statistics sub-schema: keeps only skipping-eligible leaf
   * columns, replaces field names with physical names, and sets all fields nullable.
   */
  private def getMinMaxStatsSchema(schema: StructType): Option[StructType] = {
    val fields = schema.fields.flatMap {
      case f @ StructField(_, dataType: StructType, _, _) =>
        getMinMaxStatsSchema(dataType).map(newDt =>
          StructField(DeltaColumnMapping.getPhysicalName(f), newDt))
      case f @ StructField(_, SkippingEligibleDataType(dataType), _, _) =>
        Some(StructField(DeltaColumnMapping.getPhysicalName(f), dataType))
      case _ => None
    }
    if (fields.nonEmpty) Some(StructType(fields)) else None
  }

  /**
   * Computes the null-count statistics sub-schema: every leaf column becomes
   * LongType, field names are replaced with physical names.
   */
  private def getNullCountSchema(schema: StructType): Option[StructType] = {
    val fields = schema.fields.flatMap {
      case f @ StructField(_, dataType: StructType, _, _) =>
        getNullCountSchema(dataType).map(newDt =>
          StructField(DeltaColumnMapping.getPhysicalName(f), newDt))
      case f: StructField =>
        Some(StructField(DeltaColumnMapping.getPhysicalName(f), LongType))
    }
    if (fields.nonEmpty) Some(StructType(fields)) else None
  }
}
