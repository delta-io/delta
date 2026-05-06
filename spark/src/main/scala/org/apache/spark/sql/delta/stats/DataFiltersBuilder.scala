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

import org.apache.spark.sql.delta.skipping.clustering.ClusteringColumnInfo
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaLog}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaDataSkippingType.DeltaDataSkippingType
import org.apache.spark.sql.delta.stats.DeltaStatistics._
import org.apache.spark.sql.delta.stats.DataSkippingReader._
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
// scalastyle:on import.ordering.noEmptyLine
// scalastyle:on import.ordering.wrongOrderInGroup

/**
 * Builds data filters for data skipping. Extracted from DataSkippingReaderBase
 * for readability (the original file was 3500+ lines).
 *
 * Dependencies previously accessed via enclosing trait are now explicit
 * constructor parameters.
 */
class DataFiltersBuilder(
    protected val spark: SparkSession,
    protected val dataSkippingType: DeltaDataSkippingType,
    protected val getStatsColumnOpt: StatsColumn => Option[Column],
    additionalPartitionLikeFilterSupportedExpressions: Set[String] = Set.empty,
    limitPartitionLikeFiltersToClusteringColumns: Boolean = false)
  extends DeltaLogging
{
  protected val statsProvider: StatsProvider = new StatsProvider(getStatsColumnOpt)

  protected def constructNotNullFilter(
      statsProvider: StatsProvider,
      pathToColumn: Seq[String]): Option[DataSkippingPredicate] = {
    val nullCountCol = StatsColumn(NULL_COUNT, pathToColumn, LongType)
    val numRecordsCol = StatsColumn(NUM_RECORDS, pathToColumn = Nil, LongType)
    statsProvider.getPredicateWithStatsColumnsIfExists(nullCountCol, numRecordsCol) {
      (nullCount, numRecords) => nullCount < numRecords
    }
  }

  object SkippingEligibleExpression extends GenericSkippingEligibleExpression()

  // Main function for building data filters.
  def apply(dataFilter: Expression): Option[DataSkippingPredicate] =
    constructDataFilters(dataFilter, isNullExpansionDepth = 0)

  /**
   * Helper function to construct a [[DataSkippingPredicate]] for an IsNull predicate on
   * null-intolerant expressions that are guaranteed to return non-null results for non-null
   * inputs. This method is only valid *if and only if* the passed-in expression returns null
   * for any null children. That is, if all children are non-null, the expression *must* return
   * a non-null result.
   * @param expr Expression to push down IsNull into.
   * @return A [[DataSkippingPredicate]] that's the result of pushing IsNull down into expr's
   *         children.
   */
  protected def constructIsNullFilterForNullIntolerant(
      expr: Expression,
      isNullExpansionDepth: Int): Option[DataSkippingPredicate] = {
    val filters = expr.children.map {
      // Resolve literal children directly. constructDataFilters does not support skipping on
      // literal-only children.
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
  //
  //
  // For excessively long IN-lists, we just test whether the file's min/max range overlaps the
  // range spanned by the list's smallest and largest elements.
  private def constructLiteralInListDataFilters(
      a: Expression,
      possiblyNullValues: Seq[Any],
      isNullExpansionDepth: Int): Option[DataSkippingPredicate] = {
    // The Ordering we use for sorting cannot handle null values, and these can anyway
    // be safely ignored because they will never cause an IN-list predicate to return TRUE.
    val values = possiblyNullValues.filter(_ != null)
    if (values.isEmpty) {
      // Handle the trivial empty case even for otherwise ineligible types.
      // NOTE: SQL forbids empty in-list, but InSubqueryExec could have an empty subquery result
      // or IN-list may contain only NULLs.
      return Some(DataSkippingPredicate(falseLiteral))
    }

    val (pathToColumn, dt, builder) = SkippingEligibleExpression.unapply(a).getOrElse {
      // The expression is not eligible for skipping, and we can stop constructing data filters
      // for the expression by simply returning None.
      return None
    }

    lazy val ordering = TypeUtils.getInterpretedOrdering(dt)
    if (!SkippingEligibleDataType(dt)) {
      // Don't waste time building expressions for incompatible types
      None
    }
    else {
      // Emit filters for an imprecise range test that covers the entire entire list.
      val min = Literal(values.min(ordering), dt)
      val max = Literal(values.max(ordering), dt)
      constructDataFilters(
        And(GreaterThanOrEqual(max, a), LessThanOrEqual(min, a)), isNullExpansionDepth)
    }
  }

  /**
   * Returns a file skipping predicate expression, derived from the user query, which uses column
   * statistics to prune away files that provably contain no rows the query cares about.
   *
   * Specifically, the filter extraction code must obey the following rules:
   *
   * 1. Given a query predicate `e`, `constructDataFilters(e)` must return TRUE for a file unless
   *    we can prove `e` will not return TRUE for any row the file might contain. For example,
   *    given `a = 3` and min/max stat values [0, 100], this skipping predicate is safe:
   *
   *      AND(minValues.a <= 3, maxValues.a >= 3)
   *
   *    Because that condition must be true for any file that might possibly contain `a = 3`; the
   *    skipping predicate could return FALSE only if the max is too low, or the min too high; it
   *    could return NULL only if a is NULL in every row of the file. In both latter cases, it is
   *    safe to skip the file because `a = 3` can never evaluate to TRUE.
   *
   * 2. It is unsafe to apply skipping to operators that can evaluate to NULL or produce an error
   *    for non-NULL inputs. For example, consider this query predicate involving integer
   *    addition:
   *
   *      a + 1 = 3
   *
   *    It might be tempting to apply the standard equality skipping predicate:
   *
   *      AND(minValues.a + 1 <= 3, 3 <= maxValues.a + 1)
   *
   *    However, the skipping predicate would be unsound, because the addition operator could
   *    trigger integer overflow (e.g. minValues.a = 0 and maxValues.a = INT_MAX), even though the
   *    file could very well contain rows satisfying a + 1 = 3.
   *
   * 3. Predicates involving NOT are ineligible for skipping, because
   *    `Not(constructDataFilters(e))` is seldom equivalent to `constructDataFilters(Not(e))`.
   *    For example, consider the query predicate:
   *
   *      NOT(a = 1)
   *
   *    A simple inversion of the data skipping predicate would be:
   *
   *      NOT(AND(minValues.a <= 1, maxValues.a >= 1))
   *      ==> OR(NOT(minValues.a <= 1), NOT(maxValues.a >= 1))
   *      ==> OR(minValues.a > 1, maxValues.a < 1)
   *
   *    By contrast, if we first combine the NOT with = to obtain
   *
   *      a != 1
   *
   *    We get a different skipping predicate:
   *
   *      NOT(AND(minValues.a = 1, maxValues.a = 1))
   *      ==> OR(NOT(minValues.a = 1), NOT(maxValues.a = 1))
   *      ==>  OR(minValues.a != 1, maxValues.a != 1)
   *
   *    A truth table confirms that the first (naively inverted) skipping predicate is incorrect:
   *
   *      minValues.a
   *      | maxValues.a
   *      | | OR(minValues.a > 1, maxValues.a < 1)
   *      | | | OR(minValues.a != 1, maxValues.a != 1)
   *      0 0 T T
   *      0 1 F T    !! first predicate wrongly skipped a = 0
   *      1 1 F F
   *
   *    Fortunately, we may be able to eliminate NOT from some (branches of some) predicates:
   *
   *    a. It is safe to push the NOT into the children of AND and OR using de Morgan's Law, e.g.
   *
   *         NOT(AND(a, b)) ==> OR(NOT(a), NOT(B)).
   *
   *    b. It is safe to fold NOT into other operators, when a negated form of the operator
   *       exists:
   *
   *         NOT(NOT(x)) ==> x
   *         NOT(a == b) ==> a != b
   *         NOT(a > b) ==> a <= b
   *
   * NOTE: The skipping predicate must handle the case where min and max stats for a column are
   * both NULL -- which indicates that all values in the file are NULL. Fortunately, most of the
   * operators we support data skipping for are NULL intolerant, and thus trivially satisfy this
   * requirement because they never return TRUE for NULL inputs. The only NULL tolerant operator
   * we support -- IS [NOT] NULL -- is specifically NULL aware. AND and OR are also considered
   * null tolerant, and have special-cased handling of null pushdowns.
   *
   * NOTE: The skipping predicate does *NOT* need to worry about missing stats columns (which also
   * manifest as NULL). That case is handled separately by `verifyStatsForFilter` (which disables
   * skipping for any file that lacks the needed stats columns).
   *
   * @return An optional data skipping predicate, if this function returns None, then this means
   * that the dataFilter Expression is not eligible for data skipping, i.e. we cannot skip any
   * files.
   */
  private[stats] def constructDataFilters(
      dataFilter: Expression,
      isNullExpansionDepth: Integer): Option[DataSkippingPredicate] = dataFilter match {
    // Expressions that contain only literals are not eligible for skipping.
    case cmp: Expression if cmp.children.forall(areAllLeavesLiteral) => None

    // Push skipping predicate generation through the AND:
    //
    // constructDataFilters(AND(a, b))
    // ==> AND(constructDataFilters(a), constructDataFilters(b))
    //
    // To see why this transformation is safe, consider that `constructDataFilters(a)` must
    // evaluate to TRUE *UNLESS* we can prove that `a` would not evaluate to TRUE for any row the
    // file might contain. Thus, if the rewritten form of the skipping predicate does not evaluate
    // to TRUE, at least one of the skipping predicates must not have evaluated to TRUE, which in
    // turn means we were able to prove that `a` and/or `b` will not evaluate to TRUE for any row
    // of the file. If that is the case, then `AND(a, b)` also cannot evaluate to TRUE for any row
    // of the file, which proves we have a valid data skipping predicate.
    //
    // NOTE: AND is special -- we can safely skip the file if one leg does not evaluate to TRUE,
    // even if we cannot construct a skipping filter for the other leg.
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

    // Use deMorgan's law to push the NOT past the AND. This is safe even with SQL tri-valued
    // logic (see below), and is desirable because we cannot generally push predicate filters
    // through NOT, but we *CAN* push predicate filters through AND and OR:
    //
    // constructDataFilters(NOT(AND(a, b)))
    // ==> constructDataFilters(OR(NOT(a), NOT(b)))
    // ==> OR(constructDataFilters(NOT(a)), constructDataFilters(NOT(b)))
    //
    // Assuming we can push the resulting NOT operations all the way down to some leaf operation
    // it can fold into, the rewrite allows us to create a data skipping filter from the
    // expression.
    //
    // a b AND(a, b)
    // | | | NOT(AND(a, b))
    // | | | | OR(NOT(a), NOT(b))
    // T T T F F
    // T F F T T
    // T N N N N
    // F F F T T
    // F N F T T
    // N N N N N
    case Not(And(e1, e2)) =>
      constructDataFilters(Or(Not(e1), Not(e2)), isNullExpansionDepth)

    // Push skipping predicate generation through OR (similar to AND case).
    //
    // constructDataFilters(OR(a, b))
    // ==> OR(constructDataFilters(a), constructDataFilters(b))
    //
    // Similar to AND case, if the rewritten predicate does not evaluate to TRUE, then it means
    // that neither `constructDataFilters(a)` nor `constructDataFilters(b)` evaluated to TRUE,
    // which in turn means that neither `a` nor `b` could evaluate to TRUE for any row the file
    // might contain, which proves we have a valid data skipping predicate.
    //
    // Unlike AND, a single leg of an OR expression provides no filtering power -- we can only
    // reject a file if both legs evaluate to false.
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

    // Similar to AND, we can (and want to) push the NOT past the OR using deMorgan's law.
    case Not(Or(e1, e2)) =>
      constructDataFilters(And(Not(e1), Not(e2)), isNullExpansionDepth)

    // Match any file whose null count is larger than zero.
    // Note DVs might result in a redundant read of a file.
    // However, they cannot lead to a correctness issue.
    case IsNull(SkippingEligibleColumn(a, dt)) =>
      statsProvider.getPredicateWithStatTypeIfExists(a, dt, NULL_COUNT) { nullCount =>
        nullCount > Literal(0L)
      }
    // For these null-intolerant expressions, any null input resolves into a null output. In
    // addition, these expressions are special in that a null output is only possible if one of
    // the inputs was a NULL. Push down the IsNull operator to all children and return a
    // DataSkippingPredicate that's the Or of all child expressions.
    case IsNull(e @ (_: GreaterThan | _: GreaterThanOrEqual | _: LessThan | _: LessThanOrEqual |
        _: EqualTo | _: Not | _: StartsWith)) if spark.conf.get(
          DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_ENABLED) =>
      constructIsNullFilterForNullIntolerant(e, isNullExpansionDepth)
    // And and Or necessitate custom pushdown logic for IsNull, as both expressions are not
    // considered null intolerant. Note that since the child expressions are duplicated in the
    // expanded expression, we need to track the depth of the expansion in this function's
    // signature to avoid exponential growth of the expression tree if the child expressions are
    // themselves And/Or expressions.
    case IsNull(And(left, right)) if spark.conf.get(
        DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_ENABLED) && (isNullExpansionDepth <=
          spark.conf.get(DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_MAX_DEPTH)) =>
      // The result of an AND is only Null if either operand is a Null, and the other operand is
      // True.
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
      // The result of an OR is only Null if either operand is a Null, _and_ neither operand is
      // true.
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

    // Match any file whose null count is less than the row count.
    case IsNotNull(SkippingEligibleColumn(a, _)) =>
      constructNotNullFilter(statsProvider, a)

    case Not(IsNotNull(e)) =>
      constructDataFilters(IsNull(e), isNullExpansionDepth)

    // Match any file whose min/max range contains the requested point.
    case EqualTo(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.equalTo(statsProvider, c, v)
    case EqualTo(v: Literal, a) =>
      constructDataFilters(EqualTo(a, v), isNullExpansionDepth)

    // Match any file whose min/max range contains anything other than the rejected point.
    case Not(EqualTo(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v))) =>
      builder.notEqualTo(statsProvider, c, v)
    case Not(EqualTo(v: Literal, a)) =>
      constructDataFilters(Not(EqualTo(a, v)), isNullExpansionDepth)

    // Rewrite `EqualNullSafe(a, NotNullLiteral)` as
    // `And(IsNotNull(a), EqualTo(a, NotNullLiteral))` and rewrite `EqualNullSafe(a, null)` as
    // `IsNull(a)` to let the existing logic handle it.
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

    // Match any file whose min is less than the requested upper bound.
    case LessThan(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.lessThan(statsProvider, c, v)
    case LessThan(v: Literal, a) =>
      constructDataFilters(GreaterThan(a, v), isNullExpansionDepth)
    case Not(LessThan(a, b)) =>
      constructDataFilters(GreaterThanOrEqual(a, b), isNullExpansionDepth)

    // Match any file whose min is less than or equal to the requested upper bound
    case LessThanOrEqual(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.lessThanOrEqual(statsProvider, c, v)
    case LessThanOrEqual(v: Literal, a) =>
      constructDataFilters(GreaterThanOrEqual(a, v), isNullExpansionDepth)
    case Not(LessThanOrEqual(a, b)) =>
      constructDataFilters(GreaterThan(a, b), isNullExpansionDepth)

    // Match any file whose max is larger than the requested lower bound.
    case GreaterThan(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.greaterThan(statsProvider, c, v)
    case GreaterThan(v: Literal, a) =>
      constructDataFilters(LessThan(a, v), isNullExpansionDepth)
    case Not(GreaterThan(a, b)) =>
      constructDataFilters(LessThanOrEqual(a, b), isNullExpansionDepth)

    // Match any file whose max is larger than or equal to the requested lower bound.
    case GreaterThanOrEqual(
    SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.greaterThanOrEqual(statsProvider, c, v)
    case GreaterThanOrEqual(v: Literal, a) =>
      constructDataFilters(LessThanOrEqual(a, v), isNullExpansionDepth)
    case Not(GreaterThanOrEqual(a, b)) =>
      constructDataFilters(LessThan(a, b), isNullExpansionDepth)

    // Similar to an equality test, except comparing against a prefix of the min/max stats, and
    // neither commutative nor invertible.
    case StartsWith(SkippingEligibleColumn(a, _), v @ Literal(s: UTF8String, dt: StringType)) =>
      statsProvider.getPredicateWithStatTypesIfExists(a, dt, MIN, MAX) { (min, max) =>
        val sLen = s.numChars()
        substring(min, 0, sLen) <= v && substring(max, 0, sLen) >= v
      }

    // We can only handle-IN lists whose values can all be statically evaluated to literals.
    case in @ In(a, values) if in.inSetConvertible =>
      constructLiteralInListDataFilters(
        a, values.map(_.asInstanceOf[Literal].value), isNullExpansionDepth)

    // The optimizer automatically converts all but the shortest eligible IN-lists to InSet.
    case InSet(a, values) =>
      constructLiteralInListDataFilters(a, values.toSeq, isNullExpansionDepth)

    // Treat IN(... subquery ...) as a normal IN-list, since the subquery already ran before now.
    case in: InSubqueryExec =>
      // At this point the subquery has been materialized, but values() can return None if
      // the subquery was bypassed at runtime.
      in.values().flatMap(v =>
        constructLiteralInListDataFilters(in.child, v.toSeq, isNullExpansionDepth))


    // Remove redundant pairs of NOT
    case Not(Not(e)) =>
      constructDataFilters(e, isNullExpansionDepth)

    // WARNING: NOT is dangerous, because `Not(constructDataFilters(e))` is seldom equivalent to
    // `constructDataFilters(Not(e))`. We must special-case every `Not(e)` we wish to support.
    case Not(_) => None

    // Unknown expression type... can't use it for data skipping.
    case _ => None
  }

  // Lightweight wrapper to represent a fully resolved reference to an attribute for
  // partition-like data filters. Contains the min/max/null count stats column expressions and
  // the referenced stats column for the attribute.
  private case class ResolvedPartitionLikeReference(
      referencedStatsCols: Seq[StatsColumn],
      minExpr: Expression,
      maxExpr: Expression,
      nullCountExpr: Expression)

  /**
   * Whitelist of expressions that can be rewritten as partition-like.
   * Set to a finite list to avoid having to silently introducing correctness issues as new
   * expressions that violate the assumptions of partition-like skipping are introduced.
   * There's no need to include [[SkippingEligibleColumn]] here - it's already handled explicitly.
   *
   * The following expressions have been intentionally excluded from the whitelist of supported
   * expressions:
   *  - [[AttributeReference]]: Any non-skipping eligible column references can't be rewritten as
   *    partition-like.
   *  - Any nondeterministic expression: The value returned while skipping might be different when
   *    the expression is evaluated again. For example, rand() > 0.5 would return ~25% of records
   *    if used in data skipping, while the user would expect ~50% of records to be returned.
   *  - [[UserDefinedExpression]]: Often nondeterministic, and may have side effects when executed
   *    multiple times.
   *  - [[RegExpReplace]], [[RegExpExtractBase]], [[Like]], [[MultiLikeBase]], [[InvokeLike]], and
   *    [[JsonToStructs]]: These expressions might be very expensive to evalute more than once.
   */
  private def shouldRewriteAsPartitionLike(expr: Expression): Boolean = expr match {
    // Expressions supported by traditional data skipping.
    // Boolean operators.
    case _: Not | _: Or | _: And => true
    // Comparison operators.
    case _: EqualNullSafe | _: EqualTo | _: GreaterThan | _: GreaterThanOrEqual | _: IsNull |
         _: IsNotNull | _: LessThan | _: LessThanOrEqual => true
    // String and set operators. InSubqueryExec is explicitly handled by the caller.
    case _: In | _: InSet | _: StartsWith => true
    case _: Literal => true

    // Expressions only supported for partition-like data skipping.
    // Date and time conversions.
    case _: ConvertTimezone | _: DateFormatClass | _: Extract | _: GetDateField |
         _: GetTimeField | _: IntegralToTimestampBase | _: MakeDate | _: MakeTimestamp |
         _: ParseToDate | _: ParseToTimestamp | _: ToTimestamp | _: TruncDate |
         _: TruncTimestamp | _: UTCTimestamp => true
    // Unix date and timestamp conversions.
    case _: DateFromUnixDate | _: FromUnixTime | _: TimestampToLongBase | _: ToUnixTimestamp |
         _: UnixDate | _: UnixTime | _: UnixTimestamp => true
    // Date and time arithmetic.
    case expr if DateTimeExpressionShims.isDateTimeArithmeticExpression(expr) => true
    // String expressions.
    case _: Base64 | _: BitLength | _: Chr | _: ConcatWs | _: Decode | _: Elt | _: Empty2Null |
         _: Encode | _: FormatNumber | _: FormatString | _: ILike | _: InitCap | _: Left |
         _: Length | _: Levenshtein | _: Luhncheck | _: OctetLength | _: Overlay | _: Right |
         _: Sentences | _: SoundEx | _: SplitPart | _: String2StringExpression |
         _: String2TrimExpression | _: StringDecode | _: StringInstr | _: StringLPad |
         _: StringLocate | _: StringPredicate | _: StringRPad | _: StringRepeat |
         _: StringReplace | _: StringSpace | _: StringSplit | _: StringSplitSQL |
         _: StringTranslate | _: StringTrimBoth | _: Substring | _: SubstringIndex | _: ToBinary |
         _: TryToBinary | _: UnBase64 => true
    // Arithmetic expressions.
    case _: Abs | _: BinaryArithmetic | _: Greatest | _: Least | _: UnaryMinus |
         _: UnaryPositive => true
    // Array expressions.
    case _: ArrayBinaryLike | _: ArrayCompact | _: ArrayContains | _: ArrayInsert | _: ArrayJoin |
         _: ArrayMax | _: ArrayMin | _: ArrayPosition | _: ArrayRemove | _: ArrayRepeat |
         _: ArraySetLike | _: ArraySize | _: ArraysZip |
         _: BinaryArrayExpressionWithImplicitCast | _: Concat | _: CreateArray | _: ElementAt |
         _: Flatten | _: Get | _: GetArrayItem | _: GetArrayStructFields |
         _: Reverse | _: Sequence | _: Size | _: Slice | _: SortArray | _: TryElementAt => true
    // Map expressions.
    case _: CreateMap | _: GetMapValue | _: MapConcat | _: MapContainsKey | _: MapEntries |
         _: MapFromArrays | _: MapFromEntries | _: MapKeys | _: MapValues | _: StringToMap => true
    // Struct expressions.
    case _: CreateNamedStruct | _: DropField | _: GetStructField | _: UpdateFields |
         _: WithField => true
    // Hash expressions.
    case _: Crc32 | _: HashExpression[_] | _: Md5 | _: Sha1 | _: Sha2 => true
    // URL expressions.
    case _: ParseUrl | _: UrlDecode | _: UrlEncode => true
    // NULL expressions.
    case _: AtLeastNNonNulls | _: Coalesce | _: IsNaN | _: NaNvl | _: NullIf | _: Nvl |
         _: Nvl2 => true
    // Cast expressions.
    case _: Cast | _: UpCast => true
    // Conditional expressions.
    case _: If | _: CaseWhen => true
    case _: Alias => true

    // Don't attempt partition-like skipping on any unknown expressions: there's no way to
    // guarantee it's safe to do so.
    case _ => additionalPartitionLikeFilterSupportedExpressions.contains(
      expr.getClass.getCanonicalName)
  }

  /**
   * Rewrites the references in an expression to point to the collected stats over that column
   * (if possible).
   *
   * This is generally equivalent to [[DeltaLog.rewritePartitionFilters]], with a few differences:
   * 1. This method checks the eligibility of the column datatype before rewriting it to point to
   *    the stats column (which isn't needed for partition columns).
   * 2. There's no need to handle scalar subqueries (other than InSubqueryExec) here - subqueries
   *    other than InSubqueryExec aren't eligible for data filtering.
   * 3. AND expressions may be partially rewritten as partition-like data filters if one branch
   *    is eligible but the other is not.
   *
   * For example:
   *  CAST(a AS DATE) = '2024-09-11' -> CAST(parsed_stats[minValues][a] AS DATE) = '2024-09-11'
   *
   * @param expr    The expression to rewrite.
   * @param clusteringColumnPaths The logical paths to the clustering columns in the table.
   * @return        If the expression is safe to rewrite, return the rewritten expression and a
   *                set of referenced attributes (with both the logical path to the column and the
   *                column type).
   */
  private def rewriteDataFiltersAsPartitionLikeInternal(
      expr: Expression,
      clusteringColumnPaths: Set[Seq[String]])
  : Option[(Expression, Set[ResolvedPartitionLikeReference])] = expr match {
    // The expression is an eligible reference to an attribute.
    // Do NOT allow partition-like filtering on timestamp columns because timestamps are truncated
    // to millisecond precision, meaning that we can't guarantee that the collected minVal and
    // maxVal are the same.
    // Applying these partition-like filters will generally only be beneficial if a large
    // percentage of files have the same min-max value. As a rough heuristic, only allow rewriting
    // expressions that reference only the clustering columns (since these columns are more likely
    // to have the same min-max values).
    case SkippingEligibleColumn(c, SkippingEligibleDataType(dt))
      if dt != TimestampType && dt != TimestampNTZType &&
        (!limitPartitionLikeFiltersToClusteringColumns ||
          clusteringColumnPaths.exists(SchemaUtils.areLogicalNamesEqual(_, c.reverse))) =>
      // Only rewrite the expression if all stats are collected for this column.
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
    // For other attribute references, we can't safely rewrite the expression.
    case SkippingEligibleColumn(_, _) => None
    // Explicitly disallow rewriting nondeterministic expressions. Even though this check isn't
    // strictly necessary (there shouldn't be any nondeterministic expressions in the whitelist),
    // defensively keep it due to the extreme risk of correctness issues if any nondeterministic
    // expressions sneak into the whitelist.
    case other if !other.deterministic => None
    // Inline subquery results to support InSet. The subquery should generally have already been
    // evaluated.
    case in: InSubqueryExec =>
      // Values may not be defined if the subquery has been skipped - we can't apply this filter.
      in.values().flatMap { possiblyNullValues =>
        // Rewrite the children of InSubqueryExec, then replace the subquery with an InSet
        // containing the materialized values.
        rewriteDataFiltersAsPartitionLikeInternal(in.child, clusteringColumnPaths).flatMap {
          case (rewrittenChildren, referencedStats) =>
            Some(InSet(rewrittenChildren, possiblyNullValues.toSet), referencedStats)
        }
      }
    // For all other eligible expressions, recursively rewrite the children.
    case other if shouldRewriteAsPartitionLike(other) =>
      val childResults = other.children.map(
        rewriteDataFiltersAsPartitionLikeInternal(_, clusteringColumnPaths))
      Option.whenNot (childResults.exists(_.isEmpty)) {
        val (children, stats) = childResults.map(_.get).unzip
        (other.withNewChildren(children), stats.flatten.toSet)
      }
    // Don't attempt rewriting any non-whitelisted expressions.
    case _ => None
  }

  /**
   * Returns an expression that returns true if a file must be read because of a mismatched
   * min-max value or partial nulls on a given column. For these files, it's not safe to apply
   * arbitrary partition-like filters.
   */
  private def fileMustBeScanned(
      resolvedPartitionLikeReference: ResolvedPartitionLikeReference,
      numRecordsColOpt: Option[Column]): Expression = {
    // Construct an expression to determine if all records in the file are null.
    val nullCountExpr = resolvedPartitionLikeReference.nullCountExpr
    val allNulls = numRecordsColOpt match {
      case Some(physicalNumRecords) => EqualTo(nullCountExpr, physicalNumRecords.expr)
      case _ => Literal(false)
    }

    // Note that there are 2 other differences in behavior between unpartitioned and partitioned
    // tables:
    // 1. If the column is a timestamp, the min-max stats are truncated to millisecond precision.
    //    We shouldn't apply partition-like filters in this case, but
    //    rewriteDataFiltersAsPartitionLikeInternal validates the column is not a Timestamp,
    //    so we don't have to check here.
    // 2. The min-max stats on a string column might be truncated for an unpartitioned table.
    //    Note that just validating that the min and max are equal is enough to prevent this case
    //    - if the string is truncated, the collected max value is guaranteed to be longer than
    //    the min value due to the tiebreaker character(s) appended at the end of the max.
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

  /**
   * Rewrites the given expression as a partition-like expression if possible:
   * 1. Rewrite the attribute references in the expression to reference the collected min stats
   *     on the attribute reference's column.
   * 2. Construct an expression that returns true if any of the referenced columns are not
   *     partition-like on a given file.
   * The rewritten expression is a union of the above expressions: a file is read if it's either
   * not partition-like on any of the columns or if the rewritten expression evaluates to true.
   *
   * @param clusteringColumns   The columns that are used for clustering.
   * @param expr                The data filtering expression to rewrite.
   * @return                    If the expression is safe to rewrite, return the rewritten
   *                            expression. Otherwise, return None.
   */
  def rewriteDataFiltersAsPartitionLike(
      clusteringColumns: Seq[String], expr: Expression): Option[DataSkippingPredicate] = {
    val clusteringColumnPaths =
      clusteringColumns.map(UnresolvedAttribute.quotedString(_).nameParts).toSet
    rewriteDataFiltersAsPartitionLikeInternal(expr, clusteringColumnPaths).map {
      case (newExpr, referencedStats) =>
        // Create an expression that returns true if a file must be read because it has mismatched
        // min-max values or partial nulls on any of the referenced columns.
        val numRecordsStatsCol = StatsColumn(NUM_RECORDS, pathToColumn = Nil, LongType)
        val numRecordsColOpt = getStatsColumnOpt(numRecordsStatsCol)
        val statsCols = referencedStats.flatMap(_.referencedStatsCols) + numRecordsStatsCol
        val mustScanFileExpression = referencedStats.map { resolvedReference =>
          fileMustBeScanned(resolvedReference, numRecordsColOpt)
        }.toSeq.reduceLeftOption { (l, r) => Or(l, r) }.getOrElse(Literal(false))

        // Only evaluate the rewritten expression if the file passes the validation expression,
        // ensuring that any non-partition-like input (that might cause a filter evaluation
        // exception) is skipped. Note that we cannot rely on short-circuiting here, since
        // common subexpression elimination during codegen may move the evaluation of the
        // condition before that of the file validation expression, so we need to explicitly use
        // a conditional expression to guarantee the correct evaluation order.
        val finalExpr = If(mustScanFileExpression, Literal(true), newExpr)

        // Create the final data skipping expression - read a file either if it's has nulls on any
        // referenced column, has mismatched stats on any referenced column, or the filter
        // expression evaluates to `true`.
        DataSkippingPredicate(Column(finalExpr), statsCols.toSet)
    }
  }

  // We are doing the iterative approach because of stack depth concerns.
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

