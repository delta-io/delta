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

package org.apache.spark.sql.delta.perf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaTable, Snapshot}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.stats.DeltaScanGenerator
import org.apache.spark.sql.delta.stats.DeltaStatistics.TIGHT_BOUNDS
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.Date
import java.util.Locale

/** Optimize COUNT, MIN and MAX expressions on Delta tables.
 * This optimization is only applied when the following conditions are met:
 * - The MIN/MAX columns are not nested and data type is supported by the optimization (ByteType,
 * ShortType, IntegerType, LongType, FloatType, DoubleType, DateType).
 * - All AddFiles in the Delta Log must have stats on columns used in MIN/MAX expressions,
 * or the columns must be partitioned, in the latter case it uses partitionValues, a required field.
 * - For MIN/MAX, the per-column stat used must be trustworthy across the whole table. Per the
 * Delta protocol, files with `stats.tightBounds = false` (only emitted by writers that add
 * deletion vectors without recomputing stats) have wide bounds: their `stat_min` is a lower
 * bound on the file's actual min and their `stat_max` is an upper bound on the file's actual
 * max. The optimization can still apply when wide-bound files are present, provided the
 * resulting table-wide value would have come from a tight-bound file. Concretely, for each
 * column the rule checks that `min(stat_min over all files) IS NOT DISTINCT FROM
 * min(stat_min over tight files only)` (and symmetric for MAX). When that holds, the wide
 * files do not contribute to the table-wide bound, so the result is exact. MIN and MAX are
 * evaluated independently per column.
 * - COUNT has no DISTINCT.
 * - Query has no filters.
 * - Query has no GROUP BY.
 * Example of valid query: SELECT COUNT(*), MIN(id), MAX(partition_col) FROM MyDeltaTable
 */
trait OptimizeMetadataOnlyDeltaQuery extends Logging {
  def optimizeQueryWithMetadata(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithSubqueries {
      case agg@MetadataOptimizableAggregate(tahoeLogFileIndex) =>
        createLocalRelationPlan(agg, tahoeLogFileIndex)
    }
  }

  protected def getDeltaScanGenerator(index: TahoeLogFileIndex): DeltaScanGenerator

  private def createLocalRelationPlan(
      plan: Aggregate,
      tahoeLogFileIndex: TahoeLogFileIndex): LogicalPlan = {

    val aggColumnsNames = Set(extractMinMaxFieldNames(plan).map(_.toLowerCase(Locale.ROOT)) : _*)
    val (rowCount, columnStats) = extractCountMinMaxFromDeltaLog(tahoeLogFileIndex, aggColumnsNames)

    def checkMinStatsExists(attrRef: AttributeReference): Boolean = {
      columnStats.contains(attrRef.name) &&
        columnStats(attrRef.name).min.isDefined &&
        // Avoid StructType, it is not supported by this optimization.
        // Sanity check only. If reference is nested column it would be GetStructType
        // instead of AttributeReference.
        attrRef.references.size == 1 && attrRef.references.head.dataType != StructType
    }

    def checkMaxStatsExists(attrRef: AttributeReference): Boolean = {
      columnStats.contains(attrRef.name) &&
        columnStats(attrRef.name).max.isDefined &&
        attrRef.references.size == 1 && attrRef.references.head.dataType != StructType
    }

    def convertValueIfRequired(attrRef: AttributeReference, value: Any): Any = {
      if (attrRef.dataType == DateType && value != null) {
        DateTimeUtils.fromJavaDate(value.asInstanceOf[Date])
      } else {
        value
      }
    }

    val rewrittenAggregationValues = plan.aggregateExpressions.collect {
      case Alias(AggregateExpression(
      Count(Seq(Literal(1, _))), Complete, false, None, _), _) if rowCount.isDefined =>
        rowCount.get
      case Alias(tps@ToPrettyString(AggregateExpression(
      Count(Seq(Literal(1, _))), Complete, false, None, _), _), _) if rowCount.isDefined =>
        tps.copy(child = Literal(rowCount.get)).eval()
      case Alias(AggregateExpression(
      Min(minReference: AttributeReference), Complete, false, None, _), _)
        if checkMinStatsExists(minReference) =>
        convertValueIfRequired(minReference, columnStats(minReference.name).min.get)
      case Alias(tps@ToPrettyString(AggregateExpression(
      Min(minReference: AttributeReference), Complete, false, None, _), _), _)
        if checkMinStatsExists(minReference) =>
          val v = columnStats(minReference.name).min.get
          tps.copy(child = Literal(v)).eval()
      case Alias(AggregateExpression(
      Max(maxReference: AttributeReference), Complete, false, None, _), _)
        if checkMaxStatsExists(maxReference) =>
        convertValueIfRequired(maxReference, columnStats(maxReference.name).max.get)
      case Alias(tps@ToPrettyString(AggregateExpression(
      Max(maxReference: AttributeReference), Complete, false, None, _), _), _)
        if checkMaxStatsExists(maxReference) =>
          val v = columnStats(maxReference.name).max.get
          tps.copy(child = Literal(v)).eval()
    }

    if (plan.aggregateExpressions.size == rewrittenAggregationValues.size) {
      val r = LocalRelation(
        plan.output,
        Seq(InternalRow.fromSeq(rewrittenAggregationValues)))
      r
    } else {
      logInfo(log"Query can't be optimized using metadata because stats are missing")
      plan
    }
  }

  private def extractMinMaxFieldNames(plan: Aggregate): Seq[String] = {
    plan.aggregateExpressions.collect {
      case Alias(AggregateExpression(
        Min(minReference: AttributeReference), _, _, _, _), _) =>
        minReference.name
      case Alias(AggregateExpression(
        Max(maxReference: AttributeReference), _, _, _, _), _) =>
        maxReference.name
      case Alias(ToPrettyString(AggregateExpression(
        Min(minReference: AttributeReference), _, _, _, _), _), _) =>
        minReference.name
      case Alias(ToPrettyString(AggregateExpression(
        Max(maxReference: AttributeReference), _, _, _, _), _), _) =>
        maxReference.name
    }
  }

  /**
   * Min and max values from Delta Log stats or partitionValues. Each side is `None` when the
   * corresponding aggregate cannot be answered from metadata for that column (e.g. some file
   * has stat bounds wide enough to potentially set the table-wide min/max). `Some(value)`
   * carries the computed bound; `value` may be `null` when the column is all-null.
   */
  case class DeltaColumnStat(min: Option[Any], max: Option[Any])

  private def extractCountMinMaxFromStats(
      deltaScanGenerator: DeltaScanGenerator,
      lowerCaseColumnNames: Set[String]): (Option[Long], Map[String, DeltaColumnStat]) = {
    val snapshot = deltaScanGenerator.snapshotToScan

    // Count - account for deleted rows according to deletion vectors
    val dvCardinality = coalesce(col("deletionVector.cardinality"), lit(0))
    val numLogicalRecords = (col("stats.numRecords") - dvCardinality).as("numLogicalRecords")

    val filesWithStatsForScan = deltaScanGenerator.filesWithStatsForScan(Nil)

    // The `tightBounds` field is only present in the parsed stats schema when the table was
    // written with deletion-vector support. When the field is absent, the protocol mandates
    // that every file be treated as having tight bounds.
    val hasTightBoundsField = filesWithStatsForScan.schema("stats").dataType match {
      case s: StructType => s.fieldNames.contains(TIGHT_BOUNDS)
      case _ => false
    }
    // Per-file `tight_bounds` flag projection. NULL or absent `tightBounds` is treated as
    // tight per the Delta protocol; only an explicit `false` marks the file as wide.
    val tightBoundsCol = if (hasTightBoundsField) {
      coalesce(col(s"stats.$TIGHT_BOUNDS"), lit(true))
    } else {
      lit(true)
    }

    // Validate all the files has stats
    val filesStatsCount = filesWithStatsForScan.select(
      sum(numLogicalRecords).as("numLogicalRecords"),
      count(when(col("stats.numRecords").isNull, 1)).as("missingNumRecords"),
      count(when(col("stats.numRecords") > 0, 1)).as("countNonEmptyFiles")).head

    // If any numRecords is null, we have incomplete stats;
    val allRecordsHasStats = filesStatsCount.getAs[Long]("missingNumRecords") == 0
    if (!allRecordsHasStats) {
      return (None, Map.empty)
    }
    // the sum agg is either null (for an empty table) or gives an accurate record count.
    val numRecords = if (filesStatsCount.isNullAt(0)) 0 else filesStatsCount.getLong(0)
    lazy val numFiles: Long = filesStatsCount.getAs[Long]("countNonEmptyFiles")

    val dataColumns = snapshot.statCollectionPhysicalSchema.filter(col =>
      lowerCaseColumnNames.contains(col.name.toLowerCase(Locale.ROOT)))

    // DELETE operations creates AddFile records with 0 rows, and no column stats.
    // We can safely ignore it since there is no data.
    lazy val files = filesWithStatsForScan.filter(col("stats.numRecords") > 0)
    lazy val statsMinMaxNullColumns = files.select(col("stats.*"))

    val minColName = "minValues"
    val maxColName = "maxValues"
    val nullColName = "nullCount"

    if (dataColumns.isEmpty
      || dataColumns.size != lowerCaseColumnNames.size
      || numFiles == 0
      || !statsMinMaxNullColumns.columns.contains(minColName)
      || !statsMinMaxNullColumns.columns.contains(maxColName)
      || !statsMinMaxNullColumns.columns.contains(nullColName)) {
      return (Some(numRecords), Map.empty)
    }

    // dataColumns can contain columns without stats if dataSkippingNumIndexedCols
    // has been increased
    val columnsWithStats = files.select(
      col(s"stats.$minColName.*"),
      col(s"stats.$maxColName.*"),
      col(s"stats.$nullColName.*"))
      .columns.groupBy(identity).mapValues(_.size)
      .filter(x => x._2 == 3) // 3: minValues, maxValues, nullCount
      .map(x => x._1).toSet

    // Creates a tuple with physical name to avoid recalculating it multiple times
    val dataColumnsWithStats = dataColumns.map(x => (x, DeltaColumnMapping.getPhysicalName(x)))
      .filter(x => columnsWithStats.contains(x._2))

    val columnsToQuery = dataColumnsWithStats.flatMap { columnAndPhysicalName =>
      val dataType = columnAndPhysicalName._1.dataType
      val physicalName = columnAndPhysicalName._2

      Seq(col(s"stats.$minColName.`$physicalName`").cast(dataType).as(s"min.$physicalName"),
        col(s"stats.$maxColName.`$physicalName`").cast(dataType).as(s"max.$physicalName"),
        col(s"stats.$nullColName.`$physicalName`").as(s"null_count.$physicalName"))
    } ++ Seq(
      col(s"stats.numRecords").as(s"numRecords"),
      tightBoundsCol.as("tight_bounds"))

    val minMaxExpr = dataColumnsWithStats.flatMap { columnAndPhysicalName =>
      val physicalName = columnAndPhysicalName._2

      // To validate if the column has stats for one direction (MIN or MAX) we do two
      // validations followed by a tight-bounds check:
      // 1-) COUNT(null_count.columnName) should be equals to numFiles,
      // since null_count is always non-null.
      // 2-) For MIN, the number of files with non-null min:
      // a. count(min.columnName) +
      // the number of files where all rows are NULL:
      // b. count of (ISNULL(min.columnName) and null_count.columnName == numRecords)
      // should be equals to numFiles. Symmetric check for MAX.
      // 3-) Tight-bounds check. Wide-bound files have stat_min <= actual min and
      // stat_max >= actual max, so the table-wide MIN/MAX over all files' stats might be
      // looser than the true table-wide MIN/MAX. We can still trust the value when it
      // equals the same aggregate computed over only tight files: that proves the wide
      // files cannot push the bound, so the answer is exact. Null-safe `<=>` handles
      // empty / all-null cases (both sides NULL means the column is all-null overall).
      Seq(
        s"""case when $numFiles = count(`null_count.$physicalName`)
            | AND $numFiles = (count(`min.$physicalName`) + sum(case when
            |  ISNULL(`min.$physicalName`) and `null_count.$physicalName` = numRecords
            |   then 1 else 0 end))
            | AND min(`min.$physicalName`) <=>
            |   min(case when `tight_bounds` then `min.$physicalName` end)
            | then TRUE else FALSE end as `min_complete_$physicalName`""".stripMargin,
        s"""case when $numFiles = count(`null_count.$physicalName`)
            | AND $numFiles = (count(`max.$physicalName`) + sum(case when
            |  ISNULL(`max.$physicalName`) AND `null_count.$physicalName` = numRecords
            |   then 1 else 0 end))
            | AND max(`max.$physicalName`) <=>
            |   max(case when `tight_bounds` then `max.$physicalName` end)
            | then TRUE else FALSE end as `max_complete_$physicalName`""".stripMargin,
        s"min(`min.$physicalName`) as `min_$physicalName`",
        s"max(`max.$physicalName`) as `max_$physicalName`")
    }

    val statsResults = files.select(columnsToQuery: _*).selectExpr(minMaxExpr: _*).head

    (Some(numRecords), dataColumnsWithStats
      .flatMap { columnAndPhysicalName =>
        val column = columnAndPhysicalName._1
        val physicalName = columnAndPhysicalName._2
        val minComplete = statsResults.getAs[Boolean](s"min_complete_$physicalName")
        val maxComplete = statsResults.getAs[Boolean](s"max_complete_$physicalName")
        if (!minComplete && !maxComplete) {
          None
        } else {
          // Use explicit Some(...) to preserve all-null state. Option(...) would
          // wrongly turn an all-null optimizable column into None.
          val minValue =
            if (minComplete) Some(statsResults.getAs[Any](s"min_$physicalName")) else None
          val maxValue =
            if (maxComplete) Some(statsResults.getAs[Any](s"max_$physicalName")) else None
          Some(column.name -> DeltaColumnStat(minValue, maxValue))
        }
      }.toMap)
  }

  private def extractMinMaxFromPartitionValue(
      snapshot: Snapshot,
      lowerCaseColumnNames: Set[String]): Map[String, DeltaColumnStat] = {

    val partitionedColumns = snapshot.metadata.partitionSchema
      .filter(col => lowerCaseColumnNames.contains(col.name.toLowerCase(Locale.ROOT)))
      .map(col => (col, DeltaColumnMapping.getPhysicalName(col)))

    if (partitionedColumns.isEmpty) {
      Map.empty
    } else {
      val partitionedColumnsValues = partitionedColumns.map { partitionedColumn =>
        val physicalName = partitionedColumn._2
        col(s"partitionValues.`$physicalName`")
          .cast(partitionedColumn._1.dataType).as(physicalName)
      }

      val partitionedColumnsAgg = partitionedColumns.flatMap { partitionedColumn =>
        val physicalName = partitionedColumn._2

        Seq(min(s"`$physicalName`").as(s"min_$physicalName"),
          max(s"`$physicalName`").as(s"max_$physicalName"))
      }

      val partitionedColumnsQuery = snapshot.allFiles
        .select(partitionedColumnsValues: _*)
        .agg(partitionedColumnsAgg.head, partitionedColumnsAgg.tail: _*)
        .head()

      partitionedColumns.map { partitionedColumn =>
        val physicalName = partitionedColumn._2

        partitionedColumn._1.name ->
          DeltaColumnStat(
            // Partition values are exact (no wide-bounds notion applies), so both
            // directions are always optimizable.
            Some(partitionedColumnsQuery.getAs[Any](s"min_$physicalName")),
            Some(partitionedColumnsQuery.getAs[Any](s"max_$physicalName")))
      }.toMap
    }
  }

  /**
  * Extract the Count, Min and Max values from Delta Log stats and partitionValues.
  * The first field is the rows count in the table or `None` if we cannot calculate it from stats
  * If the column is not partitioned, the values are extracted from stats when it exists.
  * If the column is partitioned, the values are extracted from partitionValues.
  */
  private def extractCountMinMaxFromDeltaLog(
      tahoeLogFileIndex: TahoeLogFileIndex,
      lowerCaseColumnNames: Set[String]):
  (Option[Long], CaseInsensitiveMap[DeltaColumnStat]) = {
    val deltaScanGen = getDeltaScanGenerator(tahoeLogFileIndex)

    val partitionedValues = extractMinMaxFromPartitionValue(
      deltaScanGen.snapshotToScan,
      lowerCaseColumnNames)

    val partitionedColNames = partitionedValues.keySet.map(_.toLowerCase(Locale.ROOT))
    val dataColumnNames = lowerCaseColumnNames -- partitionedColNames
    val (rowCount, columnStats) = extractCountMinMaxFromStats(deltaScanGen, dataColumnNames)

    (rowCount, CaseInsensitiveMap(columnStats ++ partitionedValues))
  }

  object MetadataOptimizableAggregate {

    /** Only data type that are stored in stats without any loss of precision are supported. */
    def isSupportedDataType(dataType: DataType): Boolean = {
      // DecimalType is not supported because not all the values are correctly stored
      // For example -99999999999999999999999999999999999999 in stats is -1e38
      (dataType.isInstanceOf[NumericType] && !dataType.isInstanceOf[DecimalType]) ||
      dataType.isInstanceOf[DateType]
    }

    private def getAggFunctionOptimizable(
        aggExpr: AggregateExpression): Option[DeclarativeAggregate] = {

      aggExpr match {
        case AggregateExpression(
          c@Count(Seq(Literal(1, _))), Complete, false, None, _) =>
            Some(c)
        case AggregateExpression(
          min@Min(minExpr), Complete, false, None, _) if isSupportedDataType(minExpr.dataType) =>
            Some(min)
        case AggregateExpression(
          max@Max(maxExpr), Complete, false, None, _) if isSupportedDataType(maxExpr.dataType) =>
            Some(max)
        case _ => None
      }
    }

    private def isStatsOptimizable(aggExprs: Seq[Expression]): Boolean = aggExprs.forall {
      case Alias(aggExpr: AggregateExpression, _) => getAggFunctionOptimizable(aggExpr).isDefined
      case Alias(ToPrettyString(aggExpr: AggregateExpression, _), _) =>
        getAggFunctionOptimizable(aggExpr).isDefined
      case _ => false
    }

    private def fieldsAreAttributeReference(fields: Seq[NamedExpression]): Boolean = fields.forall {
      // Fields should be AttributeReference to avoid getting the incorrect column name
      // from stats when we create the Local Relation, example
      // SELECT MAX(Column2) FROM (SELECT Column1 AS Column2 FROM TableName)
      // the AggregateExpression contains a reference to Column2, instead of Column1
      case _: AttributeReference => true
      case _ => false
    }

    def unapply(plan: Aggregate): Option[TahoeLogFileIndex] = {
      // GROUP BY is not supports. All AggregateExpression must be stats optimizable.
      if (plan.groupingExpressions.nonEmpty ||
        plan.aggregateExpressions.isEmpty ||
        !isStatsOptimizable(plan.aggregateExpressions)) {
        return None
      }
      plan.child match {
        case PhysicalOperation(fields, Nil, DeltaTable(fileIndex: TahoeLogFileIndex))
          if fileIndex.partitionFilters.isEmpty && fieldsAreAttributeReference(fields) =>
          Some(fileIndex)
        case DeltaTable(fileIndex: TahoeLogFileIndex) if fileIndex.partitionFilters.isEmpty =>
          // When all columns are selected, there are no Project/PhysicalOperation
          Some(fileIndex)
        case _ =>
          None
      }
    }
  }
}
