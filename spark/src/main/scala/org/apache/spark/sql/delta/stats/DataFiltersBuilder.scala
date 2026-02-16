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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaDataSkippingType.DeltaDataSkippingType
import org.apache.spark.sql.delta.stats.DeltaStatistics._

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.functions.{lit, substring}
import org.apache.spark.sql.types.{LongType, StringType, TimestampNTZType, TimestampType}
import org.apache.spark.sql.util.ScalaExtensions._
import org.apache.spark.unsafe.types.UTF8String
// scalastyle:on import.ordering.noEmptyLine

/**
 * Builds data-skipping filter predicates from user query expressions.
 *
 * This class was extracted from `DataSkippingReaderBase` to be a standalone,
 * independently instantiable class that both V1 and V2 can use.
 *
 * It contains:
 *   - The core `constructDataFilters` recursive rewrite engine
 *   - Partition-like rewrite for clustered tables
 *   - Helper methods (IN-list, IsNull pushdown, etc.)
 *
 * External dependencies are injected through constructor parameters:
 *   - `getStatsColumnOpt`: resolves stat column references (V1 uses column-mapping,
 *     V2 uses simple paths)
 *   - `constructNotNullFilter`: builds IsNotNull predicates
 *
 * @param spark                SparkSession (for DeltaSQLConf access)
 * @param dataSkippingType     The type of data skipping being performed
 * @param getStatsColumnOpt    Function to resolve a StatsColumn to a Column expression
 * @param limitPartitionLikeFiltersToClusteringColumns Whether to limit partition-like
 *                             rewrites to clustering columns only
 * @param additionalPartitionLikeFilterSupportedExpressions Extra expression class names
 *                             allowed for partition-like rewrite
 */
private[delta] class DataFiltersBuilder(
    protected val spark: SparkSession,
    protected val dataSkippingType: DeltaDataSkippingType,
    getStatsColumnOpt: StatsColumn => Option[Column],
    constructNotNullFilter: (StatsProvider, Seq[String]) => Option[DataSkippingPredicate],
    limitPartitionLikeFiltersToClusteringColumns: Boolean,
    additionalPartitionLikeFilterSupportedExpressions: Set[String])
{
  protected val statsProvider: StatsProvider = new StatsProvider(getStatsColumnOpt)

  protected object SkippingEligibleExpression extends GenericSkippingEligibleExpression()

  protected val trueLiteral: Column = Column(TrueLiteral)
  protected val falseLiteral: Column = Column(FalseLiteral)

  // Main function for building data filters.
  def apply(dataFilter: Expression): Option[DataSkippingPredicate] =
    constructDataFilters(dataFilter, isNullExpansionDepth = 0)

  /**
   * Helper function to construct a [[DataSkippingPredicate]] for an IsNull predicate on
   * null-intolerant expressions.
   */
  protected def constructIsNullFilterForNullIntolerant(
      expr: Expression,
      isNullExpansionDepth: Int): Option[DataSkippingPredicate] = {
    val filters = expr.children.map {
      case l: Literal =>
        if (l.value == null) {
          Some(DataSkippingPredicate(trueLiteral))
        } else {
          Some(DataSkippingPredicate(falseLiteral))
        }
      case c => constructDataFilters(IsNull(c), isNullExpansionDepth)
    }
    filters.reduceOption { (a, b) =>
      (a, b) match {
        case (Some(a), Some(b)) =>
          Some(DataSkippingPredicate(a.expr || b.expr, a.referencedStats ++ b.referencedStats))
        case _ => None
      }
    }.flatten
  }

  // Helper method for expression types that represent an IN-list of literal values.
  private def constructLiteralInListDataFilters(
      a: Expression,
      possiblyNullValues: Seq[Any],
      isNullExpansionDepth: Int): Option[DataSkippingPredicate] = {
    val values = possiblyNullValues.filter(_ != null)
    if (values.isEmpty) {
      return Some(DataSkippingPredicate(falseLiteral))
    }

    val (pathToColumn, dt, builder) = SkippingEligibleExpression.unapply(a).getOrElse {
      return None
    }

    lazy val ordering = TypeUtils.getInterpretedOrdering(dt)
    if (!SkippingEligibleDataType(dt)) {
      None
    }
    else {
      val min = Literal(values.min(ordering), dt)
      val max = Literal(values.max(ordering), dt)
      constructDataFilters(
        And(GreaterThanOrEqual(max, a), LessThanOrEqual(min, a)), isNullExpansionDepth)
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Returns a file skipping predicate expression, derived from the user query, which uses column
   * statistics to prune away files that provably contain no rows the query cares about.
   */
  private[stats] def constructDataFilters(
      dataFilter: Expression,
      isNullExpansionDepth: Integer): Option[DataSkippingPredicate] = dataFilter match {
    case cmp: Expression if cmp.children.forall(areAllLeavesLiteral) => None

    case And(e1, e2) =>
      val e1Filter = constructDataFilters(e1, isNullExpansionDepth)
      val e2Filter = constructDataFilters(e2, isNullExpansionDepth)
      if (e1Filter.isDefined && e2Filter.isDefined) {
        Some(DataSkippingPredicate(
          e1Filter.get.expr && e2Filter.get.expr,
          e1Filter.get.referencedStats ++ e2Filter.get.referencedStats))
      } else if (e1Filter.isDefined) {
        e1Filter
      } else {
        e2Filter  // possibly None
      }

    case Not(And(e1, e2)) =>
      constructDataFilters(Or(Not(e1), Not(e2)), isNullExpansionDepth)

    case Or(e1, e2) =>
      val e1Filter = constructDataFilters(e1, isNullExpansionDepth)
      val e2Filter = constructDataFilters(e2, isNullExpansionDepth)
      if (e1Filter.isDefined && e2Filter.isDefined) {
        Some(DataSkippingPredicate(
          e1Filter.get.expr || e2Filter.get.expr,
          e1Filter.get.referencedStats ++ e2Filter.get.referencedStats))
      } else {
        None
      }

    case Not(Or(e1, e2)) =>
      constructDataFilters(And(Not(e1), Not(e2)), isNullExpansionDepth)

    case IsNull(SkippingEligibleColumn(a, dt)) =>
      statsProvider.getPredicateWithStatTypeIfExists(a, dt, NULL_COUNT) { nullCount =>
        nullCount > Literal(0L)
      }

    case IsNull(e @ (_: GreaterThan | _: GreaterThanOrEqual | _: LessThan | _: LessThanOrEqual |
        _: EqualTo | _: Not | _: StartsWith)) if spark.conf.get(
          DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_ENABLED) =>
      constructIsNullFilterForNullIntolerant(e, isNullExpansionDepth)

    case IsNull(And(left, right)) if spark.conf.get(
        DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_ENABLED) && (isNullExpansionDepth <=
          spark.conf.get(DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_MAX_DEPTH)) =>
      constructDataFilters(
        And(
          Or(IsNull(left), IsNull(right)),
          Not(
            Or(
              EqualNullSafe(left, FalseLiteral),
              EqualNullSafe(right, FalseLiteral)
            )
          )
        ),
        isNullExpansionDepth = isNullExpansionDepth + 1
      )

    case IsNull(Or(left, right)) if spark.conf.get(
        DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_ENABLED) && (isNullExpansionDepth <=
          spark.conf.get(DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_MAX_DEPTH)) =>
      constructDataFilters(
        And(
          Or(IsNull(left), IsNull(right)),
          Not(
            Or(
              EqualNullSafe(left, TrueLiteral),
              EqualNullSafe(right, TrueLiteral)
            )
          )
        ),
        isNullExpansionDepth = isNullExpansionDepth + 1
      )

    case Not(IsNull(e)) =>
      constructDataFilters(IsNotNull(e), isNullExpansionDepth)

    case IsNotNull(SkippingEligibleColumn(a, _)) =>
      constructNotNullFilter(statsProvider, a)

    case Not(IsNotNull(e)) =>
      constructDataFilters(IsNull(e), isNullExpansionDepth)

    case EqualTo(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.equalTo(statsProvider, c, v)
    case EqualTo(v: Literal, a) =>
      constructDataFilters(EqualTo(a, v), isNullExpansionDepth)

    case Not(EqualTo(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v))) =>
      builder.notEqualTo(statsProvider, c, v)
    case Not(EqualTo(v: Literal, a)) =>
      constructDataFilters(Not(EqualTo(a, v)), isNullExpansionDepth)

    case EqualNullSafe(a, v: Literal) =>
      val rewrittenExpr = if (v.value != null) And(IsNotNull(a), EqualTo(a, v)) else IsNull(a)
      constructDataFilters(rewrittenExpr, isNullExpansionDepth)
    case EqualNullSafe(v: Literal, a) =>
      constructDataFilters(EqualNullSafe(a, v), isNullExpansionDepth)
    case Not(EqualNullSafe(a, v: Literal)) =>
      val rewrittenExpr = if (v.value != null) And(IsNotNull(a), EqualTo(a, v)) else IsNull(a)
      constructDataFilters(Not(rewrittenExpr), isNullExpansionDepth)
    case Not(EqualNullSafe(v: Literal, a)) =>
      constructDataFilters(Not(EqualNullSafe(a, v)), isNullExpansionDepth)

    case LessThan(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.lessThan(statsProvider, c, v)
    case LessThan(v: Literal, a) =>
      constructDataFilters(GreaterThan(a, v), isNullExpansionDepth)
    case Not(LessThan(a, b)) =>
      constructDataFilters(GreaterThanOrEqual(a, b), isNullExpansionDepth)

    case LessThanOrEqual(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.lessThanOrEqual(statsProvider, c, v)
    case LessThanOrEqual(v: Literal, a) =>
      constructDataFilters(GreaterThanOrEqual(a, v), isNullExpansionDepth)
    case Not(LessThanOrEqual(a, b)) =>
      constructDataFilters(GreaterThan(a, b), isNullExpansionDepth)

    case GreaterThan(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.greaterThan(statsProvider, c, v)
    case GreaterThan(v: Literal, a) =>
      constructDataFilters(LessThan(a, v), isNullExpansionDepth)
    case Not(GreaterThan(a, b)) =>
      constructDataFilters(LessThanOrEqual(a, b), isNullExpansionDepth)

    case GreaterThanOrEqual(
    SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.greaterThanOrEqual(statsProvider, c, v)
    case GreaterThanOrEqual(v: Literal, a) =>
      constructDataFilters(LessThanOrEqual(a, v), isNullExpansionDepth)
    case Not(GreaterThanOrEqual(a, b)) =>
      constructDataFilters(LessThan(a, b), isNullExpansionDepth)

    case StartsWith(SkippingEligibleColumn(a, _), v @ Literal(s: UTF8String, dt: StringType)) =>
      statsProvider.getPredicateWithStatTypesIfExists(a, dt, MIN, MAX) { (min, max) =>
        val sLen = s.numChars()
        substring(min, 0, sLen) <= v && substring(max, 0, sLen) >= v
      }

    case in @ In(a, values) if in.inSetConvertible =>
      constructLiteralInListDataFilters(
        a, values.map(_.asInstanceOf[Literal].value), isNullExpansionDepth)

    case InSet(a, values) =>
      constructLiteralInListDataFilters(a, values.toSeq, isNullExpansionDepth)

    case in: InSubqueryExec =>
      in.values().flatMap(v =>
        constructLiteralInListDataFilters(in.child, v.toSeq, isNullExpansionDepth))

    case Not(Not(e)) =>
      constructDataFilters(e, isNullExpansionDepth)

    case Not(_) => None

    case _ => None
  }
  // scalastyle:on line.size.limit

  // ==================== Partition-like rewrite (V1-specific, also usable by V2) =================

  private case class ResolvedPartitionLikeReference(
      referencedStatsCols: Seq[StatsColumn],
      minExpr: Expression,
      maxExpr: Expression,
      nullCountExpr: Expression)

  private def shouldRewriteAsPartitionLike(expr: Expression): Boolean = expr match {
    case _: Not | _: Or | _: And => true
    case _: EqualNullSafe | _: EqualTo | _: GreaterThan | _: GreaterThanOrEqual | _: IsNull |
         _: IsNotNull | _: LessThan | _: LessThanOrEqual => true
    case _: In | _: InSet | _: StartsWith => true
    case _: Literal => true
    case _: ConvertTimezone | _: DateFormatClass | _: Extract | _: GetDateField |
         _: GetTimeField | _: IntegralToTimestampBase | _: MakeDate | _: MakeTimestamp |
         _: ParseToDate | _: ParseToTimestamp | _: ToTimestamp | _: TruncDate |
         _: TruncTimestamp | _: UTCTimestamp => true
    case _: DateFromUnixDate | _: FromUnixTime | _: TimestampToLongBase | _: ToUnixTimestamp |
         _: UnixDate | _: UnixTime | _: UnixTimestamp => true
    case expr if DateTimeExpressionShims.isDateTimeArithmeticExpression(expr) => true
    case _: Base64 | _: BitLength | _: Chr | _: ConcatWs | _: Decode | _: Elt | _: Empty2Null |
         _: Encode | _: FormatNumber | _: FormatString | _: ILike | _: InitCap | _: Left |
         _: Length | _: Levenshtein | _: Luhncheck | _: OctetLength | _: Overlay | _: Right |
         _: Sentences | _: SoundEx | _: SplitPart | _: String2StringExpression |
         _: String2TrimExpression | _: StringDecode | _: StringInstr | _: StringLPad |
         _: StringLocate | _: StringPredicate | _: StringRPad | _: StringRepeat |
         _: StringReplace | _: StringSpace | _: StringSplit | _: StringSplitSQL |
         _: StringTranslate | _: StringTrimBoth | _: Substring | _: SubstringIndex | _: ToBinary |
         _: TryToBinary | _: UnBase64 => true
    case _: Abs | _: BinaryArithmetic | _: Greatest | _: Least | _: UnaryMinus |
         _: UnaryPositive => true
    case _: ArrayBinaryLike | _: ArrayCompact | _: ArrayContains | _: ArrayInsert | _: ArrayJoin |
         _: ArrayMax | _: ArrayMin | _: ArrayPosition | _: ArrayRemove | _: ArrayRepeat |
         _: ArraySetLike | _: ArraySize | _: ArraysZip |
         _: BinaryArrayExpressionWithImplicitCast | _: Concat | _: CreateArray | _: ElementAt |
         _: Flatten | _: Get | _: GetArrayItem | _: GetArrayStructFields |
         _: Reverse | _: Sequence | _: Size | _: Slice | _: SortArray | _: TryElementAt => true
    case _: CreateMap | _: GetMapValue | _: MapConcat | _: MapContainsKey | _: MapEntries |
         _: MapFromArrays | _: MapFromEntries | _: MapKeys | _: MapValues | _: StringToMap => true
    case _: CreateNamedStruct | _: DropField | _: GetStructField | _: UpdateFields |
         _: WithField => true
    case _: Crc32 | _: HashExpression[_] | _: Md5 | _: Sha1 | _: Sha2 => true
    case _: ParseUrl | _: UrlDecode | _: UrlEncode => true
    case _: AtLeastNNonNulls | _: Coalesce | _: IsNaN | _: NaNvl | _: NullIf | _: Nvl |
         _: Nvl2 => true
    case _: Cast | _: UpCast => true
    case _: If | _: CaseWhen => true
    case _: Alias => true
    case _ => additionalPartitionLikeFilterSupportedExpressions.contains(
      expr.getClass.getCanonicalName)
  }

  private def rewriteDataFiltersAsPartitionLikeInternal(
      expr: Expression,
      clusteringColumnPaths: Set[Seq[String]])
  : Option[(Expression, Set[ResolvedPartitionLikeReference])] = expr match {
    case SkippingEligibleColumn(c, SkippingEligibleDataType(dt))
      if dt != TimestampType && dt != TimestampNTZType &&
        (!limitPartitionLikeFiltersToClusteringColumns ||
          clusteringColumnPaths.exists(SchemaUtils.areLogicalNamesEqual(_, c.reverse))) =>
      val minStatsCol = StatsColumn(MIN, c, dt)
      val maxStatsCol = StatsColumn(MAX, c, dt)
      val nullCountStatsCol = StatsColumn(NULL_COUNT, c, dt)
      for {
        minCol <- getStatsColumnOpt(minStatsCol);
        maxCol <- getStatsColumnOpt(maxStatsCol);
        nullCol <- getStatsColumnOpt(nullCountStatsCol)
      } yield {
        val resolvedAttribute = ResolvedPartitionLikeReference(
          Seq(minStatsCol, maxStatsCol, nullCountStatsCol),
          minCol.expr,
          maxCol.expr,
          nullCol.expr)
        (minCol.expr, Set(resolvedAttribute))
      }
    case SkippingEligibleColumn(_, _) => None
    case other if !other.deterministic => None
    case in: InSubqueryExec =>
      in.values().flatMap { possiblyNullValues =>
        rewriteDataFiltersAsPartitionLikeInternal(in.child, clusteringColumnPaths).flatMap {
          case (rewrittenChildren, referencedStats) =>
            Some(InSet(rewrittenChildren, possiblyNullValues.toSet), referencedStats)
        }
      }
    case other if shouldRewriteAsPartitionLike(other) =>
      val childResults = other.children.map(
        rewriteDataFiltersAsPartitionLikeInternal(_, clusteringColumnPaths))
      Option.whenNot (childResults.exists(_.isEmpty)) {
        val (children, stats) = childResults.map(_.get).unzip
        (other.withNewChildren(children), stats.flatten.toSet)
      }
    case _ => None
  }

  private def fileMustBeScanned(
      resolvedPartitionLikeReference: ResolvedPartitionLikeReference,
      numRecordsColOpt: Option[Column]): Expression = {
    val nullCountExpr = resolvedPartitionLikeReference.nullCountExpr
    val allNulls = numRecordsColOpt match {
      case Some(physicalNumRecords) => EqualTo(nullCountExpr, physicalNumRecords.expr)
      case _ => Literal(false)
    }
    Not(
      Or(
        allNulls,
        And(
          EqualTo(
            resolvedPartitionLikeReference.minExpr, resolvedPartitionLikeReference.maxExpr),
          EqualTo(resolvedPartitionLikeReference.nullCountExpr, Literal(0L))
        )
      )
    )
  }

  def rewriteDataFiltersAsPartitionLike(
      clusteringColumns: Seq[String], expr: Expression): Option[DataSkippingPredicate] = {
    val clusteringColumnPaths =
      clusteringColumns.map(UnresolvedAttribute.quotedString(_).nameParts).toSet
    rewriteDataFiltersAsPartitionLikeInternal(expr, clusteringColumnPaths).map {
      case (newExpr, referencedStats) =>
        val numRecordsStatsCol = StatsColumn(NUM_RECORDS, pathToColumn = Nil, LongType)
        val numRecordsColOpt = getStatsColumnOpt(numRecordsStatsCol)
        val statsCols = referencedStats.flatMap(_.referencedStatsCols) + numRecordsStatsCol
        val mustScanFileExpression = referencedStats.map { resolvedReference =>
          fileMustBeScanned(resolvedReference, numRecordsColOpt)
        }.toSeq.reduceLeftOption { (l, r) => Or(l, r) }.getOrElse(Literal(false))

        val finalExpr = If(mustScanFileExpression, Literal(true), newExpr)
        DataSkippingPredicate(Column(finalExpr), statsCols.toSet)
    }
  }

  private[stats] def areAllLeavesLiteral(e: Expression): Boolean = {
    val stack = scala.collection.mutable.Stack[Expression]()
    def pushIfNonLiteral(e: Expression): Unit = e match {
      case _: Literal =>
      case _ => stack.push(e)
    }
    pushIfNonLiteral(e)
    while (stack.nonEmpty) {
      val children = stack.pop().children
      if (children.isEmpty) {
        return false
      }
      children.foreach(pushIfNonLiteral)
    }
    true
  }
}
