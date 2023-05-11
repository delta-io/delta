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
import java.io.Closeable

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaLog, DeltaTableUtils}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata}
import org.apache.spark.sql.delta.implicits._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaDataSkippingType.DeltaDataSkippingType
import org.apache.spark.sql.delta.stats.DeltaStatistics._
import org.apache.spark.sql.delta.util.StateCache
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{AtomicType, BooleanType, CalendarIntervalType, DataType, DateType, LongType, NumericType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Used to hold the list of files and scan stats after pruning files using the limit.
 */
case class ScanAfterLimit(
    files: Seq[AddFile],
    byteSize: Option[Long],
    numPhysicalRecords: Option[Long],
    numLogicalRecords: Option[Long])

/**
 * Used in deduplicateAndFilterRemovedLocally/getFilesAndNumRecords iterator for grouping
 * physical and logical number of records.
 *
 * @param numPhysicalRecords The number of records physically present in the file.
 * @param numLogicalRecords The physical number of records minus the Deletion Vector cardinality.
 */
case class NumRecords(numPhysicalRecords: java.lang.Long, numLogicalRecords: java.lang.Long)

/**
 * Represents a stats column (MIN, MAX, etc) for a given (nested) user table column name. Used to
 * keep track of which stats columns a data skipping query depends on.
 *
 * The `statType` is any value accepted by `getStatsColumnOpt()` (see object `DeltaStatistics`);
 * `pathToColumn` is the nested name of the user column whose stats are to be accessed.
 */
private [stats] case class StatsColumn(
    statType: String,
    pathToColumn: Seq[String] = Nil)

/**
 * A data skipping predicate, which includes the expression itself, plus the set of stats columns
 * that expression depends on. The latter is required to correctly handle missing stats, which would
 * make the predicate unreliable; for details, see `DataSkippingReader.verifyStatsForFilter`.
 *
 * NOTE: It would be more accurate to call these "file keeping" predicates, because they specify the
 * set of files a query must examine, not the set of rows a query can safely skip.
 */
private [sql] case class DataSkippingPredicate(
    expr: Column,
    referencedStats: Set[StatsColumn]
)

/**
 * Overloads the constructor for `DataSkippingPredicate`, allowing callers to pass referenced stats
 * as individual arguments, rather than wrapped up as a Set.
 *
 * For example, instead of this:
 *
 *   DataSkippingPredicate(pred, Set(stat1, stat2))
 *
 * We can just do:
 *
 *   DataSkippingPredicate(pred, stat1, stat2)
 */
private [sql] object DataSkippingPredicate {
  def apply(filters: Column, referencedStats: StatsColumn*): DataSkippingPredicate = {
    DataSkippingPredicate(filters, referencedStats.toSet)
  }
}

/**
 * An extractor that matches on access of a skipping-eligible column. We only collect stats for leaf
 * columns, so internal columns of nested types are ineligible for skipping.
 *
 * NOTE: This check is sufficient for safe use of NULL_COUNT stats, but safe use of MIN and MAX
 * stats requires additional restrictions on column data type (see SkippingEligibleLiteral).
 *
 * @return The path to the column and the column's data type if it exists and is eligible.
 *         Otherwise, return None.
 */
object SkippingEligibleColumn {
  def unapply(arg: Expression): Option[(Seq[String], DataType)] = {
    // Only atomic types are eligible for skipping, and args should always be resolved by now.
    val eligible = arg.resolved && arg.dataType.isInstanceOf[AtomicType]
    if (eligible) searchChain(arg).map(_ -> arg.dataType) else None
  }

  private def searchChain(arg: Expression): Option[Seq[String]] = arg match {
    case a: Attribute => Some(a.name :: Nil)
    case GetStructField(child, _, Some(name)) =>
      searchChain(child).map(name +: _)
    case g @ GetStructField(child, ord, None) if g.resolved =>
      searchChain(child).map(g.childSchema(ord).name +: _)
    case _ =>
      None
  }
}

/**
 * An extractor that matches on access of a skipping-eligible Literal. Delta tables track min/max
 * stats for a limited set of data types, and only Literals of those types are skipping-eligible.
 *
 * @return The Literal, if it is eligible. Otherwise, return None.
 */
object SkippingEligibleLiteral {
  def unapply(arg: Literal): Option[Column] = {
    if (SkippingEligibleDataType(arg.dataType)) Some(new Column(arg)) else None
  }
}

object SkippingEligibleDataType {
  // Call this directly, e.g. `SkippingEligibleDataType(dataType)`
  def apply(dataType: DataType): Boolean = dataType match {
    case _: NumericType | DateType | TimestampType | StringType => true
    case _ => false
  }

  // Use these in `match` statements
  def unapply(dataType: DataType): Option[DataType] = {
    if (SkippingEligibleDataType(dataType)) Some(dataType) else None
  }

  def unapply(f: StructField): Option[DataType] = unapply(f.dataType)
}

private[delta] object DataSkippingReader {

  /** Default number of cols for which we should collect stats */
  val DATA_SKIPPING_NUM_INDEXED_COLS_DEFAULT_VALUE = 32

  private[this] def col(e: Expression): Column = new Column(e)
  def fold(e: Expression): Column = col(new Literal(e.eval(), e.dataType))

  // Literals often used in the data skipping reader expressions.
  val trueLiteral: Column = col(TrueLiteral)
  val falseLiteral: Column = col(FalseLiteral)
  val nullStringLiteral: Column = col(new Literal(null, StringType))
  val nullBooleanLiteral: Column = col(new Literal(null, BooleanType))
  val oneMillisecondLiteralExpr: Literal = {
    val oneMillisecond = new CalendarInterval(0, 0, 1000 /* micros */)
    new Literal(oneMillisecond, CalendarIntervalType)
  }

  val sizeCollectorInputEncoders: Seq[Option[ExpressionEncoder[_]]] = Seq(
    Option(ExpressionEncoder[Boolean]()),
    Option(ExpressionEncoder[java.lang.Long]()),
    Option(ExpressionEncoder[java.lang.Long]()),
    Option(ExpressionEncoder[java.lang.Long]()))
}

/**
 * Adds the ability to use statistics to filter the set of files based on predicates
 * to a [[org.apache.spark.sql.delta.Snapshot]] of a given Delta table.
 */
trait DataSkippingReaderBase
  extends DeltaScanGenerator
  with StatisticsCollection
  with ReadsMetadataFields
  with StateCache
  with DeltaLogging {

  import DataSkippingReader._

  def allFiles: Dataset[AddFile]
  def path: Path
  def version: Long
  def metadata: Metadata
  private[delta] def sizeInBytesIfKnown: Option[Long]
  def deltaLog: DeltaLog
  def schema: StructType
  private[delta] def numOfFilesIfKnown: Option[Long]
  def redactedPath: String

  private def useStats = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_SKIPPING)

  /** Returns a DataFrame expression to obtain a list of files with parsed statistics. */
  private def withStatsInternal0: DataFrame = {
    allFiles.withColumn("stats", from_json(col("stats"), statsSchema))
  }

  private lazy val withStatsCache =
    cacheDS(withStatsInternal0, s"Delta Table State with Stats #$version - $redactedPath")

  protected def withStatsInternal: DataFrame = withStatsCache.getDS

  /** All files with the statistics column dropped completely. */
  def withNoStats: DataFrame = allFiles.drop("stats")

  /**
   * Returns a parsed and cached representation of files with statistics.
   *
   *
   * @return [[DataFrame]]
   */
  final def withStats: DataFrame = {
    withStatsInternal
  }

  /**
   * Constructs a [[DataSkippingPredicate]] for isNotNull predicates.
   */
   protected def constructNotNullFilter(
      statsProvider: StatsProvider,
      pathToColumn: Seq[String]): Option[DataSkippingPredicate] = {
    val nullCountCol = StatsColumn(NULL_COUNT, pathToColumn)
    val numRecordsCol = StatsColumn(NUM_RECORDS)
    statsProvider.getPredicateWithStatsColumns(nullCountCol, numRecordsCol) {
      (nullCount, numRecords) => nullCount < numRecords
    }
  }

  def withStatsDeduplicated: DataFrame = withStats

  /**
   * Builds the data filters for data skipping.
   */
  class DataFiltersBuilder(
      protected val spark: SparkSession,
      protected val dataSkippingType: DeltaDataSkippingType)
  {
    protected val statsProvider: StatsProvider = new StatsProvider(getStatsColumnOpt)

    // Main function for building data filters.
    def apply(dataFilter: Expression): Option[DataSkippingPredicate] =
      constructDataFilters(dataFilter)

    // Helper method for expression types that represent an IN-list of literal values.
    //
    //
    // For excessively long IN-lists, we just test whether the file's min/max range overlaps the
    // range spanned by the list's smallest and largest elements.
    private def constructLiteralInListDataFilters(a: Expression, possiblyNullValues: Seq[Any]):
        Option[DataSkippingPredicate] = {
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
        constructDataFilters(And(GreaterThanOrEqual(max, a), LessThanOrEqual(min, a)))
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
     * we support -- IS [NOT] NULL -- is specifically NULL aware.
     *
     * NOTE: The skipping predicate does *NOT* need to worry about missing stats columns (which also
     * manifest as NULL). That case is handled separately by `verifyStatsForFilter` (which disables
     * skipping for any file that lacks the needed stats columns).
     */
    private def constructDataFilters(dataFilter: Expression):
        Option[DataSkippingPredicate] = dataFilter match {
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
        val e1Filter = constructDataFilters(e1)
        val e2Filter = constructDataFilters(e2)
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
        constructDataFilters(Or(Not(e1), Not(e2)))

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
        val e1Filter = constructDataFilters(e1)
        val e2Filter = constructDataFilters(e2)
        if (e1Filter.isDefined && e2Filter.isDefined) {
          Some(DataSkippingPredicate(
            e1Filter.get.expr || e2Filter.get.expr,
            e1Filter.get.referencedStats ++ e2Filter.get.referencedStats))
        } else {
          None
        }

      // Similar to AND, we can (and want to) push the NOT past the OR using deMorgan's law.
      case Not(Or(e1, e2)) =>
        constructDataFilters(And(Not(e1), Not(e2)))

      // Match any file whose null count is larger than zero.
      // Note DVs might result in a redundant read of a file.
      // However, they cannot lead to a correctness issue.
      case IsNull(SkippingEligibleColumn(a, _)) =>
        statsProvider.getPredicateWithStatType(a, NULL_COUNT) { nullCount =>
          nullCount > Literal(0L)
        }
      case Not(IsNull(e)) =>
        constructDataFilters(IsNotNull(e))

      // Match any file whose null count is less than the row count.
      case IsNotNull(SkippingEligibleColumn(a, _)) =>
        constructNotNullFilter(statsProvider, a)

      case Not(IsNotNull(e)) =>
        constructDataFilters(IsNull(e))

      // Match any file whose min/max range contains the requested point.
      case EqualTo(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
        builder.equalTo(statsProvider, c, v)
      case EqualTo(v: Literal, a) =>
        constructDataFilters(EqualTo(a, v))

      // Match any file whose min/max range contains anything other than the rejected point.
      case Not(EqualTo(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v))) =>
        builder.notEqualTo(statsProvider, c, v)
      case Not(EqualTo(v: Literal, a)) =>
        constructDataFilters(Not(EqualTo(a, v)))

      // Rewrite `EqualNullSafe(a, NotNullLiteral)` as
      // `And(IsNotNull(a), EqualTo(a, NotNullLiteral))` and rewrite `EqualNullSafe(a, null)` as
      // `IsNull(a)` to let the existing logic handle it.
      case EqualNullSafe(a, v: Literal) =>
        val rewrittenExpr = if (v.value != null) And(IsNotNull(a), EqualTo(a, v)) else IsNull(a)
        constructDataFilters(rewrittenExpr)
      case EqualNullSafe(v: Literal, a) =>
        constructDataFilters(EqualNullSafe(a, v))
      case Not(EqualNullSafe(a, v: Literal)) =>
        val rewrittenExpr = if (v.value != null) And(IsNotNull(a), EqualTo(a, v)) else IsNull(a)
        constructDataFilters(Not(rewrittenExpr))
      case Not(EqualNullSafe(v: Literal, a)) =>
        constructDataFilters(Not(EqualNullSafe(a, v)))

      // Match any file whose min is less than the requested upper bound.
      case LessThan(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
        builder.lessThan(statsProvider, c, v)
      case LessThan(v: Literal, a) =>
        constructDataFilters(GreaterThan(a, v))
      case Not(LessThan(a, b)) =>
        constructDataFilters(GreaterThanOrEqual(a, b))

      // Match any file whose min is less than or equal to the requested upper bound
      case LessThanOrEqual(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
        builder.lessThanOrEqual(statsProvider, c, v)
      case LessThanOrEqual(v: Literal, a) =>
        constructDataFilters(GreaterThanOrEqual(a, v))
      case Not(LessThanOrEqual(a, b)) =>
        constructDataFilters(GreaterThan(a, b))

      // Match any file whose max is larger than the requested lower bound.
      case GreaterThan(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
        builder.greaterThan(statsProvider, c, v)
      case GreaterThan(v: Literal, a) =>
        constructDataFilters(LessThan(a, v))
      case Not(GreaterThan(a, b)) =>
        constructDataFilters(LessThanOrEqual(a, b))

      // Match any file whose max is larger than or equal to the requested lower bound.
      case GreaterThanOrEqual(
          SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
        builder.greaterThanOrEqual(statsProvider, c, v)
      case GreaterThanOrEqual(v: Literal, a) =>
        constructDataFilters(LessThanOrEqual(a, v))
      case Not(GreaterThanOrEqual(a, b)) =>
        constructDataFilters(LessThan(a, b))

      // Similar to an equality test, except comparing against a prefix of the min/max stats, and
      // neither commutative nor invertible.
      case StartsWith(SkippingEligibleColumn(a, _), v @ Literal(s: UTF8String, StringType)) =>
        statsProvider.getPredicateWithStatTypes(a, MIN, MAX) { (min, max) =>
          val sLen = s.numChars()
          substring(min, 0, sLen) <= v && substring(max, 0, sLen) >= v
        }

      // We can only handle-IN lists whose values can all be statically evaluated to literals.
      case in @ In(a, values) if in.inSetConvertible =>
        constructLiteralInListDataFilters(a, values.map(_.asInstanceOf[Literal].value))

      // The optimizer automatically converts all but the shortest eligible IN-lists to InSet.
      case InSet(a, values) =>
        constructLiteralInListDataFilters(a, values.toSeq)

      // Treat IN(... subquery ...) as a normal IN-list, since the subquery already ran before now.
      case in: InSubqueryExec =>
        // At this point the subquery has been materialized, but values() can return None if
        // the subquery was bypassed at runtime.
        in.values().flatMap(v => constructLiteralInListDataFilters(in.child, v.toSeq))


      // Remove redundant pairs of NOT
      case Not(Not(e)) =>
        constructDataFilters(e)

      // WARNING: NOT is dangerous, because `Not(constructDataFilters(e))` is seldom equivalent to
      // `constructDataFilters(Not(e))`. We must special-case every `Not(e)` we wish to support.
      case Not(_) => None

      // Unknown expression type... can't use it for data skipping.
      case _ => None
    }

    /**
     * An extractor that matches expressions that are eligible for data skipping predicates.
     *
     * @return A tuple of 1) column name referenced in the expression, 2) date type for the
     *         expression, 3) [[DataSkippingPredicateBuilder]] that builds the data skipping
     *         predicate for the expression, if the given expression is eligible.
     *         Otherwise, return None.
     */
    object SkippingEligibleExpression {
      def unapply(arg: Expression)
          : Option[(Seq[String], DataType, DataSkippingPredicateBuilder)] = arg match {
        case SkippingEligibleColumn(c, dt) =>
          Some((c, dt, DataSkippingPredicateBuilder.ColumnBuilder))
        case _ => None
      }
    }
  }

  /**
   * Returns an expression to access the given statistics for a specific column, or None if that
   * stats column does not exist.
   *
   * @param statType One of the fields declared by object `DeltaStatistics`
   * @param pathToColumn The components of the nested column name to get stats for.
   */
  final protected def getStatsColumnOpt(statType: String, pathToColumn: Seq[String] = Nil)
      : Option[Column] = {
    // If the requested stats type doesn't even exist, just return None right away. This can
    // legitimately happen if we have no stats at all, or if column stats are disabled (in which
    // case only the NUM_RECORDS stat type is available).
    if (!statsSchema.exists(_.name == statType)) {
      return None
    }

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
      Some((getBaseStatsColumn.getField(statType), statsSchema(statType).dataType, metadata.schema))
    pathToColumn
      .foldRight(initialState) {
        // NOTE: Only match on StructType, because we cannot traverse through other DataTypes.
        case (fieldName, Some((statCol, statsSchema: StructType, tableSchema: StructType))) =>
          // First try to step into the table schema
          val tableFieldOpt = tableSchema.findNestedFieldIgnoreCase(Seq(fieldName))

          // If that worked, try to step into the stats schema, using its its physical name
          val statsFieldOpt = tableFieldOpt
            .map(DeltaColumnMapping.getPhysicalName)
            .filter(physicalFieldName => statsSchema.exists(_.name == physicalFieldName))
            .map(statsSchema(_))

          // If all that succeeds, return the new stats column and the corresponding data types.
          statsFieldOpt.map(statsField =>
            (statCol.getField(statsField.name), statsField.dataType, tableFieldOpt.get.dataType))

        // Propagate failure if the above match failed (or if already None)
        case _ => None
      }
      // Filter out non-leaf columns -- they lack stats so skipping predicates can't use them.
      .filterNot(_._2.isInstanceOf[StructType])
      .map {
        case (statCol, TimestampType, _) if statType == MAX =>
          // SC-22824: For timestamps, JSON serialization will truncate to milliseconds. This means
          // that we must adjust 1 millisecond upwards for max stats, or we will incorrectly skip
          // records that differ only in microsecond precision. (For example, a file containing only
          // 01:02:03.456789 will be written with min == max == 01:02:03.456, so we must consider it
          // to contain the range from 01:02:03.456 to 01:02:03.457.)
          //
          // There is a longer term task SC-22825 to fix the serialization problem that caused this.
          // But we need the adjustment in any case to correctly read stats written by old versions.
          new Column(Cast(TimeAdd(statCol.expr, oneMillisecondLiteralExpr), TimestampType))
        case (statCol, _, _) =>
          statCol
      }
  }

  /**
   * Returns an expression to access the given statistics for a specific column, or a NULL
   * literal expression if that column does not exist.
   */
  final protected[delta] def getStatsColumnOrNullLiteral(
      statType: String,
      pathToColumn: Seq[String] = Nil) : Column =
    getStatsColumnOpt(statType, pathToColumn).getOrElse(lit(null))

  /** Overload for convenience working with StatsColumn helpers */
  final protected def getStatsColumnOpt(stat: StatsColumn): Option[Column] =
    getStatsColumnOpt(stat.statType, stat.pathToColumn)

  /** Overload for convenience working with StatsColumn helpers */
  final protected[delta] def getStatsColumnOrNullLiteral(stat: StatsColumn): Column =
    getStatsColumnOrNullLiteral(stat.statType, stat.pathToColumn)

  /**
   * Returns an expression that can be used to check that the required statistics are present for a
   * given file. If any required statistics are missing we must include the corresponding file.
   *
   * NOTE: We intentionally choose to disable skipping for any file if any required stat is missing,
   * because doing it that way allows us to check each stat only once (rather than once per
   * use). Checking per-use would anyway only help for tables where the number of indexed columns
   * has changed over time, producing add.stats_parsed records with differing schemas. That should
   * be a rare enough case to not worry about optimizing for, given that the fix requires more
   * complex skipping predicates that would penalize the common case.
   */
  protected def verifyStatsForFilter(referencedStats: Set[StatsColumn]): Column = {
    recordFrameProfile("Delta", "DataSkippingReader.verifyStatsForFilter") {
      // The NULL checks for MIN and MAX stats depend on NULL_COUNT and NUM_RECORDS. Derive those
      // implied dependencies first, so the main pass can treat them like any other column.
      //
      // NOTE: We must include explicit NULL checks on all stats columns we access here, because our
      // caller will negate the expression we return. In case a stats column is NULL, `NOT(expr)`
      // must return `TRUE`, and without these NULL checks it would instead return
      // `NOT(NULL)` => `NULL`.
      referencedStats.flatMap { stat => stat match {
        case StatsColumn(MIN, _) | StatsColumn(MAX, _) =>
          Seq(stat, StatsColumn(NULL_COUNT, stat.pathToColumn), StatsColumn(NUM_RECORDS))
        case _ =>
          Seq(stat)
      }}.map{stat => stat match {
        // A usable MIN or MAX stat must be non-NULL, unless the column is provably all-NULL
        //
        // NOTE: We don't care about NULL/missing NULL_COUNT and NUM_RECORDS here, because the
        // separate NULL checks we emit for those columns will force the overall validation
        // predicate conjunction to FALSE in that case -- AND(FALSE, <anything>) is FALSE.
        case StatsColumn(MIN, _) | StatsColumn(MAX, _) =>
          getStatsColumnOrNullLiteral(stat).isNotNull ||
            (getStatsColumnOrNullLiteral(NULL_COUNT, stat.pathToColumn) ===
              getStatsColumnOrNullLiteral(NUM_RECORDS))
        case _ =>
          // Other stats, such as NULL_COUNT and NUM_RECORDS stat, merely need to be non-NULL
          getStatsColumnOrNullLiteral(stat).isNotNull
      }}
        .reduceLeftOption(_.and(_))
        .getOrElse(trueLiteral)
    }
  }

  private def buildSizeCollectorFilter(): (ArrayAccumulator, Column => Column) = {
    val bytesCompressed = col("size")
    val rows = getStatsColumnOrNullLiteral(NUM_RECORDS)
    val dvCardinality = coalesce(col("deletionVector.cardinality"), lit(0L))
    val logicalRows = (rows - dvCardinality).as("logicalRows")

    val accumulator = new ArrayAccumulator(4)

    spark.sparkContext.register(accumulator)

    // The arguments (order and datatype) must match the encoders defined in the
    // `sizeCollectorInputEncoders` value.
    val collector = (include: Boolean,
                     bytesCompressed: java.lang.Long,
                     logicalRows: java.lang.Long,
                     rows: java.lang.Long) => {
      if (include) {
        accumulator.add((0, bytesCompressed)) /* count bytes of AddFiles */
        accumulator.add((1, Option(rows).map(_.toLong).getOrElse(-1L))) /* count rows in AddFiles */
        accumulator.add((2, 1)) /* count number of AddFiles */
        accumulator.add((3, Option(logicalRows)
          .map(_.toLong).getOrElse(-1L))) /* count logical rows in AddFiles */
      }
      include
    }
    val collectorUdf = SparkUserDefinedFunction(
      f = collector,
      dataType = BooleanType,
      inputEncoders = sizeCollectorInputEncoders,
      deterministic = false)

    (accumulator, collectorUdf(_: Column, bytesCompressed, logicalRows, rows))
  }

  override def filesWithStatsForScan(partitionFilters: Seq[Expression]): DataFrame = {
    DeltaLog.filterFileList(metadata.partitionSchema, withStats, partitionFilters)
  }

  /**
   * Get all the files in this table.
   *
   * @param keepNumRecords Also select `stats.numRecords` in the query.
   *                       This may slow down the query as it has to parse json.
   */
  protected def getAllFiles(keepNumRecords: Boolean): Seq[AddFile] = recordFrameProfile(
      "Delta", "DataSkippingReader.getAllFiles") {
    val ds = if (keepNumRecords) {
      withStats // use withStats instead of allFiles so the `stats` column is already parsed
        // keep only the numRecords field as a Json string in the stats field
        .withColumn("stats", to_json(struct(col("stats.numRecords") as 'numRecords)))
    } else {
      allFiles.withColumn("stats", nullStringLiteral)
    }
    convertDataFrameToAddFiles(ds.toDF())
  }

  /**
   * Given the partition filters on the data, rewrite these filters by pointing to the metadata
   * columns.
   */
  protected def constructPartitionFilters(filters: Seq[Expression]): Column = {
    recordFrameProfile("Delta", "DataSkippingReader.constructPartitionFilters") {
      val rewritten = DeltaLog.rewritePartitionFilters(
        metadata.partitionSchema, spark.sessionState.conf.resolver, filters)
      rewritten.reduceOption(And).map { expr => new Column(expr) }.getOrElse(trueLiteral)
    }
  }

  /**
   * Get all the files in this table given the partition filter and the corresponding size of
   * the scan.
   *
   * @param keepNumRecords Also select `stats.numRecords` in the query.
   *                       This may slow down the query as it has to parse json.
   */
  protected def filterOnPartitions(
      partitionFilters: Seq[Expression],
      keepNumRecords: Boolean): (Seq[AddFile], DataSize) = recordFrameProfile(
      "Delta", "DataSkippingReader.filterOnPartitions") {
    val df = if (keepNumRecords) {
      // use withStats instead of allFiles so the `stats` column is already parsed
      val filteredFiles =
        DeltaLog.filterFileList(metadata.partitionSchema, withStats, partitionFilters)
      filteredFiles
        // keep only the numRecords field as a Json string in the stats field
        .withColumn("stats", to_json(struct(col("stats.numRecords") as 'numRecords)))
    } else {
      val filteredFiles =
        DeltaLog.filterFileList(metadata.partitionSchema, allFiles.toDF(), partitionFilters)
      filteredFiles
        .withColumn("stats", nullStringLiteral)
    }
    val files = convertDataFrameToAddFiles(df)
    val sizeInBytesByPartitionFilters = files.map(_.size).sum
    files.toSeq -> DataSize(Some(sizeInBytesByPartitionFilters), None, Some(files.size))
  }

  /**
   * Given the partition and data filters, leverage data skipping statistics to find the set of
   * files that need to be queried. Returns a tuple of the files and optionally the size of the
   * scan that's generated if there were no filters, if there were only partition filters, and
   * combined effect of partition and data filters respectively.
   */
  protected def getDataSkippedFiles(
      partitionFilters: Column,
      dataFilters: DataSkippingPredicate,
      keepNumRecords: Boolean): (Seq[AddFile], Seq[DataSize]) = recordFrameProfile(
      "Delta", "DataSkippingReader.getDataSkippedFiles") {
    val (totalSize, totalFilter) = buildSizeCollectorFilter()
    val (partitionSize, partitionFilter) = buildSizeCollectorFilter()
    val (scanSize, scanFilter) = buildSizeCollectorFilter()

    // NOTE: If any stats are missing, the value of `dataFilters` is untrustworthy -- it could be
    // NULL or even just plain incorrect. We rely on `verifyStatsForFilter` to be FALSE in that
    // case, forcing the overall OR to evaluate as TRUE no matter what value `dataFilters` takes.
    val filteredFiles = withStats.where(
        totalFilter(trueLiteral) &&
          partitionFilter(partitionFilters) &&
          scanFilter(dataFilters.expr || !verifyStatsForFilter(dataFilters.referencedStats))
      )

    val statsColumn = if (keepNumRecords) {
      // keep only the numRecords field as a Json string in the stats field
      to_json(struct(col("stats.numRecords") as 'numRecords))
    } else nullStringLiteral

    val files =
      recordFrameProfile("Delta", "DataSkippingReader.getDataSkippedFiles.collectFiles") {
      val df = filteredFiles.withColumn("stats", statsColumn)
      convertDataFrameToAddFiles(df)
    }
    files.toSeq -> Seq(DataSize(totalSize), DataSize(partitionSize), DataSize(scanSize))
  }

  private def getCorrectDataSkippingType(
      dataSkippingType: DeltaDataSkippingType): DeltaDataSkippingType = {
    dataSkippingType
  }

  /**
   * Gathers files that should be included in a scan based on the given predicates.
   * Statistics about the amount of data that will be read are gathered and returned.
   * Note, the statistics column that is added when keepNumRecords = true should NOT
   * take into account DVs. Consumers of this method might commit the file. The semantics
   * of the statistics need to be consistent across all files.
   */
  override def filesForScan(filters: Seq[Expression], keepNumRecords: Boolean): DeltaScan = {
    val startTime = System.currentTimeMillis()
    if (filters == Seq(TrueLiteral) || filters.isEmpty || schema.isEmpty) {
      recordDeltaOperation(deltaLog, "delta.skipping.none") {
        // When there are no filters we can just return allFiles with no extra processing
        val dataSize = DataSize(
          bytesCompressed = sizeInBytesIfKnown,
          rows = None,
          files = numOfFilesIfKnown)
        return DeltaScan(
          version = version,
          files = getAllFiles(keepNumRecords),
          total = dataSize,
          partition = dataSize,
          scanned = dataSize)(
          scannedSnapshot = snapshotToScan,
          partitionFilters = ExpressionSet(Nil),
          dataFilters = ExpressionSet(Nil),
          unusedFilters = ExpressionSet(Nil),
          scanDurationMs = System.currentTimeMillis() - startTime,
          dataSkippingType = getCorrectDataSkippingType(DeltaDataSkippingType.noSkippingV1)
        )
      }
    }

    import DeltaTableUtils._
    val partitionColumns = metadata.partitionColumns

    // For data skipping, avoid using the filters that involve subqueries.

    val (subqueryFilters, flatFilters) = filters.partition {
      case f => containsSubquery(f)
    }

    val (partitionFilters, dataFilters) = flatFilters
        .partition(isPredicatePartitionColumnsOnly(_, partitionColumns, spark))

    if (dataFilters.isEmpty) recordDeltaOperation(deltaLog, "delta.skipping.partition") {
      // When there are only partition filters we can scan allFiles
      // rather than withStats and thus we skip data skipping information.
      val (files, scanSize) = filterOnPartitions(partitionFilters, keepNumRecords)
      DeltaScan(
        version = version,
        files = files,
        total = DataSize(sizeInBytesIfKnown, None, numOfFilesIfKnown),
        partition = scanSize,
        scanned = scanSize)(
        scannedSnapshot = snapshotToScan,
        partitionFilters = ExpressionSet(partitionFilters),
        dataFilters = ExpressionSet(Nil),
        unusedFilters = ExpressionSet(subqueryFilters),
        scanDurationMs = System.currentTimeMillis() - startTime,
        dataSkippingType =
          getCorrectDataSkippingType(DeltaDataSkippingType.partitionFilteringOnlyV1)
      )
    } else recordDeltaOperation(deltaLog, "delta.skipping.data") {
      val finalPartitionFilters = constructPartitionFilters(partitionFilters)

      val dataSkippingType = if (partitionFilters.isEmpty) {
        DeltaDataSkippingType.dataSkippingOnlyV1
      } else {
        DeltaDataSkippingType.dataSkippingAndPartitionFilteringV1
      }

      val (skippingFilters, unusedFilters) = if (useStats) {
        val constructDataFilters = new DataFiltersBuilder(spark, dataSkippingType)
        dataFilters.map(f => (f, constructDataFilters(f))).partition(f => f._2.isDefined)
      } else {
        (Nil, dataFilters.map(f => (f, None)))
      }

      val finalSkippingFilters = skippingFilters
        .map(_._2.get)
        .reduceOption((skip1, skip2) => DataSkippingPredicate(
          // Fold the filters into a conjunction, while unioning their referencedStats.
          skip1.expr && skip2.expr, skip1.referencedStats ++ skip2.referencedStats))
        .getOrElse(DataSkippingPredicate(trueLiteral))

      val (files, sizes) = {
        getDataSkippedFiles(finalPartitionFilters, finalSkippingFilters, keepNumRecords)
      }

      DeltaScan(
        version = version,
        files = files,
        total = sizes(0),
        partition = sizes(1),
        scanned = sizes(2))(
        scannedSnapshot = snapshotToScan,
        partitionFilters = ExpressionSet(partitionFilters),
        dataFilters = ExpressionSet(skippingFilters.map(_._1)),
        unusedFilters = ExpressionSet(unusedFilters.map(_._1) ++ subqueryFilters),
        scanDurationMs = System.currentTimeMillis() - startTime,
        dataSkippingType = getCorrectDataSkippingType(dataSkippingType)
      )
    }
  }

  /**
   * Gathers files that should be included in a scan based on the limit clause, when there is
   * no filter or projection present. Statistics about the amount of data that will be read
   * are gathered and returned.
   */
  override def filesForScan(limit: Long): DeltaScan =
    recordDeltaOperation(deltaLog, "delta.skipping.limit") {
      val startTime = System.currentTimeMillis()
      val scan = pruneFilesByLimit(withStats, limit)

      val totalDataSize = new DataSize(
        sizeInBytesIfKnown,
        None,
        numOfFilesIfKnown
      )

      val scannedDataSize = new DataSize(
        scan.byteSize,
        scan.numPhysicalRecords,
        Some(scan.files.size)
      )

      DeltaScan(
        version = version,
        files = scan.files,
        total = totalDataSize,
        partition = null,
        scanned = scannedDataSize)(
        scannedSnapshot = snapshotToScan,
        partitionFilters = ExpressionSet(Nil),
        dataFilters = ExpressionSet(Nil),
        unusedFilters = ExpressionSet(Nil),
        scanDurationMs = System.currentTimeMillis() - startTime,
        dataSkippingType = DeltaDataSkippingType.limit
      )
    }

  /**
   * Gathers files that should be included in a scan based on the given predicates and limit.
   * This will be called only when all predicates are on partitioning columns.
   * Statistics about the amount of data that will be read are gathered and returned.
   */
  override def filesForScan(limit: Long, partitionFilters: Seq[Expression]): DeltaScan =
    recordDeltaOperation(deltaLog, "delta.skipping.filteredLimit") {
      val startTime = System.currentTimeMillis()
      val finalPartitionFilters = constructPartitionFilters(partitionFilters)

      val scan = {
        pruneFilesByLimit(withStats.where(finalPartitionFilters), limit)
      }

      val totalDataSize = new DataSize(
        sizeInBytesIfKnown,
        None,
        numOfFilesIfKnown,
        None
      )

      val scannedDataSize = new DataSize(
        scan.byteSize,
        scan.numPhysicalRecords,
        Some(scan.files.size),
        scan.numLogicalRecords
      )

      DeltaScan(
        version = version,
        files = scan.files,
        total = totalDataSize,
        partition = null,
        scanned = scannedDataSize)(
        scannedSnapshot = snapshotToScan,
        partitionFilters = ExpressionSet(partitionFilters),
        dataFilters = ExpressionSet(Nil),
        unusedFilters = ExpressionSet(Nil),
        scanDurationMs = System.currentTimeMillis() - startTime,
        dataSkippingType = DeltaDataSkippingType.filteredLimit
      )
    }

  /**
   * Get AddFile (with stats) actions corresponding to given set of paths in the Snapshot.
   * If a path doesn't exist in snapshot, it will be ignored and no [[AddFile]] will be returned
   * for it.
   * @param paths Sequence of paths for which we want to get [[AddFile]] action
   * @return a sequence of addFiles for the given `paths`
   */
  def getSpecificFilesWithStats(paths: Seq[String]): Seq[AddFile] = {
    recordFrameProfile("Delta", "DataSkippingReader.getSpecificFilesWithStats") {
      val right = paths.toDF(spark, "path")
      val df = allFiles.join(right, Seq("path"), "leftsemi")
      convertDataFrameToAddFiles(df)
    }
  }

  /** Get the files and number of records within each file, to perform limit pushdown. */
  def getFilesAndNumRecords(
      df: DataFrame): Iterator[(AddFile, NumRecords)] with Closeable = recordFrameProfile(
    "Delta", "DataSkippingReaderEdge.getFilesAndNumRecords") {
    import org.apache.spark.sql.delta.implicits._

    val dvCardinality = coalesce(col("deletionVector.cardinality"), lit(0L))
    val numLogicalRecords = col("stats.numRecords") - dvCardinality

    val result = df.withColumn("numPhysicalRecords", col("stats.numRecords")) // Physical
      .withColumn("numLogicalRecords", numLogicalRecords) // Logical
      .withColumn("stats", nullStringLiteral)
      .select(struct(col("*")).as[AddFile],
        col("numPhysicalRecords").as[java.lang.Long], col("numLogicalRecords").as[java.lang.Long])
      .collectAsList()

    new Iterator[(AddFile, NumRecords)] with Closeable {
      private val underlying = result.iterator
      override def hasNext: Boolean = underlying.hasNext
      override def next(): (AddFile, NumRecords) = {
        val next = underlying.next()
        (next._1, NumRecords(numPhysicalRecords = next._2, numLogicalRecords = next._3))
      }

      override def close(): Unit = {
      }

    }
  }

  protected def convertDataFrameToAddFiles(df: DataFrame): Array[AddFile] = {
    df.as[AddFile].collect()
  }

  protected[delta] def pruneFilesByLimit(df: DataFrame, limit: Long): ScanAfterLimit = {
    val withNumRecords = {
      getFilesAndNumRecords(df)
    }

    var logicalRowsToScan = 0L
    var physicalRowsToScan = 0L
    var bytesToScan = 0L
    var bytesToIgnore = 0L
    var rowsUnknown = false

    val filesAfterLimit = try {
      val iter = withNumRecords
      val filesToScan = ArrayBuffer[AddFile]()
      val filesToIgnore = ArrayBuffer[AddFile]()
      while (iter.hasNext && logicalRowsToScan < limit) {
        val file = iter.next
        if (file._2.numPhysicalRecords == null || file._2.numLogicalRecords == null) {
          // this file has no stats, ignore for now
          bytesToIgnore += file._1.size
          filesToIgnore += file._1
        } else {
          physicalRowsToScan += file._2.numPhysicalRecords.toLong
          logicalRowsToScan += file._2.numLogicalRecords.toLong
          bytesToScan += file._1.size
          filesToScan += file._1
        }
      }

      // If the files that have stats do not contain enough rows, fall back to reading all files
      if (logicalRowsToScan < limit && filesToIgnore.nonEmpty) {
        filesToScan ++= filesToIgnore
        bytesToScan += bytesToIgnore
        rowsUnknown = true
      }
      filesToScan.toSeq
    } finally {
      withNumRecords.close()
    }

    if (rowsUnknown) {
      ScanAfterLimit(filesAfterLimit, Some(bytesToScan), None, None)
    } else {
      ScanAfterLimit(filesAfterLimit, Some(bytesToScan),
        Some(physicalRowsToScan), Some(logicalRowsToScan))
    }
  }
}

trait DataSkippingReader extends DataSkippingReaderBase
