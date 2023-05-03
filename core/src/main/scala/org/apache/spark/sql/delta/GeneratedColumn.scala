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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import java.util.Locale

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils.quoteIdentifier
import org.apache.spark.sql.delta.sources.DeltaSourceUtils.GENERATION_EXPRESSION_METADATA_KEY
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.AnalysisHelper

import org.apache.spark.sql.{AnalysisException, Column, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, CaseInsensitiveMap}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{Metadata => FieldMetadata}
/**
 * Provide utility methods to implement Generated Columns for Delta. Users can use the following
 * SQL syntax to create a table with generated columns.
 *
 * ```
 * CREATE TABLE table_identifier(
 * column_name column_type,
 * column_name column_type GENERATED ALWAYS AS ( generation_expr ),
 * ...
 * )
 * USING delta
 * [ PARTITIONED BY (partition_column_name, ...) ]
 * ```
 *
 * This is an example:
 * ```
 * CREATE TABLE foo(
 * id bigint,
 * type string,
 * subType string GENERATED ALWAYS AS ( SUBSTRING(type FROM 0 FOR 4) ),
 * data string,
 * eventTime timestamp,
 * day date GENERATED ALWAYS AS ( days(eventTime) )
 * USING delta
 * PARTITIONED BY (type, day)
 * ```
 *
 * When writing to a table, for these generated columns:
 * - If the output is missing a generated column, we will add an expression to generate it.
 * - If a generated column exists in the output, in other words, we will add a constraint to ensure
 *   the given value doesn't violate the generation expression.
 */
object GeneratedColumn extends DeltaLogging with AnalysisHelper {

  def satisfyGeneratedColumnProtocol(protocol: Protocol): Boolean =
    protocol.isFeatureSupported(GeneratedColumnsTableFeature)

  /**
   * Whether the field contains the generation expression. Note: this doesn't mean the column is a
   * generated column. A column is a generated column only if the table's
   * `minWriterVersion` >= `GeneratedColumn.MIN_WRITER_VERSION` and the column metadata contains
   * generation expressions. Use the other `isGeneratedColumn` to check whether it's a generated
   * column instead.
   */
  private[delta] def isGeneratedColumn(field: StructField): Boolean = {
    field.metadata.contains(GENERATION_EXPRESSION_METADATA_KEY)
  }

  /** Whether a column is a generated column. */
  def isGeneratedColumn(protocol: Protocol, field: StructField): Boolean = {
    satisfyGeneratedColumnProtocol(protocol) && isGeneratedColumn(field)
  }

  /**
   * Whether any generation expressions exist in the schema. Note: this doesn't mean the table
   * contains generated columns. A table has generated columns only if its protocol satisfies
   * Generated Column (listed in Table Features or supported implicitly) and some of columns in
   * the table schema contain generation expressions. Use `enforcesGeneratedColumns` to check
   * generated column tables instead.
   */
  def hasGeneratedColumns(schema: StructType): Boolean = {
    schema.exists(isGeneratedColumn)
  }

  /**
   * Returns the generated columns of a table. A column is a generated column requires:
   * - The table writer protocol >= GeneratedColumn.MIN_WRITER_VERSION;
   * - It has a generation expression in the column metadata.
   */
  def getGeneratedColumns(snapshot: Snapshot): Seq[StructField] = {
    if (satisfyGeneratedColumnProtocol(snapshot.protocol)) {
      snapshot.metadata.schema.partition(isGeneratedColumn)._1
    } else {
      Nil
    }
  }

  /**
   * Whether the table has generated columns. A table has generated columns only if its
   * protocol satisfies Generated Column (listed in Table Features or supported implicitly) and
   * some of columns in the table schema contain generation expressions.
   *
   * As Spark will propagate column metadata storing the generation expression through
   * the entire plan, old versions that don't support generated columns may create tables whose
   * schema contain generation expressions. However, since these old versions has a lower writer
   * version, we can use the table's `minWriterVersion` to identify such tables and treat them as
   * normal tables.
   *
   * @param protocol the table protocol.
   * @param metadata the table metadata.
   */
  def enforcesGeneratedColumns(protocol: Protocol, metadata: Metadata): Boolean = {
    satisfyGeneratedColumnProtocol(protocol) && metadata.schema.exists(isGeneratedColumn)
  }

  /** Return the generation expression from a field metadata if any. */
  def getGenerationExpressionStr(metadata: FieldMetadata): Option[String] = {
    if (metadata.contains(GENERATION_EXPRESSION_METADATA_KEY)) {
      Some(metadata.getString(GENERATION_EXPRESSION_METADATA_KEY))
    } else {
      None
    }
  }

  /**
   * Return the generation expression from a field if any. This method doesn't check the protocl.
   * The caller should make sure the table writer protocol meets `satisfyGeneratedColumnProtocol`
   * before calling method.
   */
  def getGenerationExpression(field: StructField): Option[Expression] = {
    getGenerationExpressionStr(field.metadata).map { exprStr =>
      parseGenerationExpression(SparkSession.active, exprStr)
    }
  }

  /** Return the generation expression from a field if any. */
  private def getGenerationExpressionStr(field: StructField): Option[String] = {
    getGenerationExpressionStr(field.metadata)
  }

  /** Parse a generation expression string and convert it to an [[Expression]] object. */
  private def parseGenerationExpression(spark: SparkSession, exprString: String): Expression = {
    spark.sessionState.sqlParser.parseExpression(exprString)
  }

  /**
   * SPARK-27561 added support for lateral column alias. This means generation expressions that
   * reference other generated columns no longer fail analysis in `validateGeneratedColumns`.
   *
   * This method checks for and throws an error if:
   * - A generated column references itself
   * - A generated column references another generated column
   */
  def validateColumnReferences(
      spark: SparkSession,
      fieldName: String,
      expression: Expression,
      schema: StructType): Unit = {
    val allowedBaseColumns = schema
      .filterNot(_.name == fieldName) // Can't reference itself
      .filterNot(isGeneratedColumn) // Can't reference other generated columns
    val relation = new LocalRelation(StructType(allowedBaseColumns).toAttributes)
    try {
      val analyzer: Analyzer = spark.sessionState.analyzer
      val analyzed = analyzer.execute(Project(Seq(Alias(expression, fieldName)()), relation))
      analyzer.checkAnalysis(analyzed)
    } catch {
      case ex: AnalysisException =>
        // Improve error message if possible
        if (ex.getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION") {
          throw DeltaErrors.generatedColumnsReferToWrongColumns(ex)
        }
        throw ex
    }
  }

  /**
   * If the schema contains generated columns, check the following unsupported cases:
   * - Refer to a non-existent column or another generated column.
   * - Use an unsupported expression.
   * - The expression type is not the same as the column type.
   */
  def validateGeneratedColumns(spark: SparkSession, schema: StructType): Unit = {
    val (generatedColumns, normalColumns) = schema.partition(isGeneratedColumn)
    // Create a fake relation using the normal columns and add a project with generation expressions
    // on top of it to ask Spark to analyze the plan. This will help us find out the following
    // errors:
    // - Refer to a non existent column in a generation expression.
    // - Refer to a generated column in another one.
    val relation = new LocalRelation(StructType(normalColumns).toAttributes)
    val selectExprs = generatedColumns.map { f =>
      getGenerationExpressionStr(f) match {
        case Some(exprString) =>
          val expr = parseGenerationExpression(spark, exprString)
          validateColumnReferences(spark, f.name, expr, schema)
          new Column(expr).alias(f.name)
        case None =>
          // Should not happen
          throw DeltaErrors.expressionsNotFoundInGeneratedColumn(f.name)
      }
    }
    val dfWithExprs = try {
      val plan = Project(selectExprs.map(_.expr.asInstanceOf[NamedExpression]), relation)
      Dataset.ofRows(spark, plan)
    } catch {
      case e: AnalysisException if e.getMessage != null =>
        val regexCandidates = Seq(
          ("A column or function parameter with name .*?cannot be resolved. " +
            "Did you mean one of the following?.*?").r,
          "cannot resolve.*?given input columns:.*?".r,
          "Column.*?does not exist.".r
        )
        if (regexCandidates.exists(_.findFirstMatchIn(e.getMessage).isDefined)) {
          throw DeltaErrors.generatedColumnsReferToWrongColumns(e)
        } else {
          throw e
        }
    }
    // Check whether the generation expressions are valid
    dfWithExprs.queryExecution.analyzed.transformAllExpressions {
      case expr: Alias =>
        // Alias will be non deterministic if it points to a non deterministic expression.
        // Skip `Alias` to provide a better error for a non deterministic expression.
        expr
      case expr @ (_: GetStructField | _: GetArrayItem) =>
        // The complex type extractors don't have a function name, so we need to check them
        // separately. `GetMapValue` and `GetArrayStructFields` are not supported because Delta
        // Invariant Check doesn't support them.
        expr
      case expr: UserDefinedExpression =>
        throw DeltaErrors.generatedColumnsUDF(expr)
      case expr if !expr.deterministic =>
        throw DeltaErrors.generatedColumnsNonDeterministicExpression(expr)
      case expr if expr.isInstanceOf[AggregateExpression] =>
        throw DeltaErrors.generatedColumnsAggregateExpression(expr)
      case expr if !SupportedGenerationExpressions.expressions.contains(expr.getClass) =>
        throw DeltaErrors.generatedColumnsUnsupportedExpression(expr)
    }
    // Compare the columns types defined in the schema and the expression types.
    generatedColumns.zip(dfWithExprs.schema).foreach { case (column, expr) =>
      if (column.dataType != expr.dataType) {
        throw DeltaErrors.generatedColumnsTypeMismatch(column.name, column.dataType, expr.dataType)
      }
    }
  }

  def getGeneratedColumnsAndColumnsUsedByGeneratedColumns(schema: StructType): Set[String] = {
    val generationExprs = schema.flatMap { col =>
      getGenerationExpressionStr(col).map { exprStr =>
        val expr = parseGenerationExpression(SparkSession.active, exprStr)
        new Column(expr).alias(col.name)
      }
    }
    if (generationExprs.isEmpty) {
      return Set.empty
    }

    val df = Dataset.ofRows(SparkSession.active, new LocalRelation(schema.toAttributes))
    val generatedColumnsAndColumnsUsedByGeneratedColumns =
      df.select(generationExprs: _*).queryExecution.analyzed match {
        case Project(exprs, _) =>
          exprs.flatMap {
            case Alias(expr, column) =>
              expr.references.map {
                case a: AttributeReference => a.name
                case other =>
                  // Should not happen since the columns should be resolved
                  throw DeltaErrors.unexpectedAttributeReference(s"$other")
              }.toSeq :+ column
            case other =>
              // Should not happen since we use `Alias` expressions.
              throw DeltaErrors.unexpectedAlias(s"$other")
          }
        case other =>
          // Should not happen since `select` should use `Project`.
          throw DeltaErrors.unexpectedProject(other.toString())
      }
    // Converting columns to lower case is fine since Delta's schema is always case insensitive.
    generatedColumnsAndColumnsUsedByGeneratedColumns.map(_.toLowerCase(Locale.ROOT)).toSet
  }

  private def createFieldPath(nameParts: Seq[String]): String = {
    nameParts.map(quoteIfNeeded _).mkString(".")
  }

  /**
   * Try to get `OptimizablePartitionExpression`s of a data column when a partition column is
   * defined as a generated column and refers to this data column.
   *
   * @param schema the table schema
   * @param partitionSchema the partition schema. If a partition column is defined as a generated
   *                        column, its column metadata should contain the generation expression.
   */
  def getOptimizablePartitionExpressions(
      schema: StructType,
      partitionSchema: StructType): Map[String, Seq[OptimizablePartitionExpression]] = {
    val partitionGenerationExprs = partitionSchema.flatMap { col =>
      getGenerationExpressionStr(col).map { exprStr =>
        val expr = parseGenerationExpression(SparkSession.active, exprStr)
        new Column(expr).alias(col.name)
      }
    }
    if (partitionGenerationExprs.isEmpty) {
      return Map.empty
    }

    val spark = SparkSession.active
    val resolver = spark.sessionState.analyzer.resolver

    // `a.name` comes from the generation expressions which users may use different cases. We
    // need to normalize it to the same case so that we can group expressions for the same
    // column name together.
    val nameNormalizer: String => String =
      if (spark.sessionState.conf.caseSensitiveAnalysis) x => x else _.toLowerCase(Locale.ROOT)

    /**
     * Returns a normalized column name with its `OptimizablePartitionExpression`
     */
    def createExpr(nameParts: Seq[String])(func: => OptimizablePartitionExpression):
      Option[(String, OptimizablePartitionExpression)] = {
      if (schema.findNestedField(nameParts, resolver = resolver).isDefined) {
        Some(nameNormalizer(createFieldPath(nameParts)) -> func)
      } else {
        None
      }
    }

    val df = Dataset.ofRows(SparkSession.active, new LocalRelation(schema.toAttributes))
    val extractedPartitionExprs =
      df.select(partitionGenerationExprs: _*).queryExecution.analyzed match {
        case Project(exprs, _) =>
          exprs.flatMap {
            case Alias(expr, partColName) =>
              expr match {
                case Cast(ExtractBaseColumn(name, TimestampType), DateType, _, _) =>
                  createExpr(name)(DatePartitionExpr(partColName))
                case Cast(ExtractBaseColumn(name, DateType), DateType, _, _) =>
                  createExpr(name)(DatePartitionExpr(partColName))
                case Year(ExtractBaseColumn(name, DateType)) =>
                  createExpr(name)(YearPartitionExpr(partColName))
                case Year(Cast(ExtractBaseColumn(name, TimestampType), DateType, _, _)) =>
                  createExpr(name)(YearPartitionExpr(partColName))
                case Year(Cast(ExtractBaseColumn(name, DateType), DateType, _, _)) =>
                  createExpr(name)(YearPartitionExpr(partColName))
                case Month(Cast(ExtractBaseColumn(name, TimestampType), DateType, _, _)) =>
                  createExpr(name)(MonthPartitionExpr(partColName))
                case DateFormatClass(
                  Cast(ExtractBaseColumn(name, DateType), TimestampType, _, _),
                      StringLiteral(format), _) =>
                    format match {
                      case DATE_FORMAT_YEAR_MONTH =>
                        createExpr(name)(
                          DateFormatPartitionExpr(partColName, DATE_FORMAT_YEAR_MONTH))
                      case _ => None
                    }
                case DateFormatClass(ExtractBaseColumn(name, TimestampType),
                    StringLiteral(format), _) =>
                  format match {
                    case DATE_FORMAT_YEAR_MONTH =>
                      createExpr(name)(
                        DateFormatPartitionExpr(partColName, DATE_FORMAT_YEAR_MONTH))
                    case DATE_FORMAT_YEAR_MONTH_DAY =>
                      createExpr(name)(
                        DateFormatPartitionExpr(partColName, DATE_FORMAT_YEAR_MONTH_DAY))
                    case DATE_FORMAT_YEAR_MONTH_DAY_HOUR =>
                      createExpr(name)(
                        DateFormatPartitionExpr(partColName, DATE_FORMAT_YEAR_MONTH_DAY_HOUR))
                    case _ => None
                  }
                case DayOfMonth(Cast(ExtractBaseColumn(name, TimestampType),
                    DateType, _, _)) =>
                  createExpr(name)(DayPartitionExpr(partColName))
                case Hour(ExtractBaseColumn(name, TimestampType), _) =>
                  createExpr(name)(HourPartitionExpr(partColName))
                case Substring(ExtractBaseColumn(name, StringType), IntegerLiteral(pos),
                    IntegerLiteral(len)) =>
                  createExpr(name)(SubstringPartitionExpr(partColName, pos, len))
                case TruncTimestamp(
                  StringLiteral(format), ExtractBaseColumn(name, TimestampType), _) =>
                    createExpr(name)(TimestampTruncPartitionExpr(format, partColName))
                case TruncTimestamp(
                  StringLiteral(format),
                  Cast(ExtractBaseColumn(name, DateType), TimestampType, _, _), _) =>
                    createExpr(name)(TimestampTruncPartitionExpr(format, partColName))
                case ExtractBaseColumn(name, _) =>
                  createExpr(name)(IdentityPartitionExpr(partColName))
                case TruncDate(ExtractBaseColumn(name, DateType), StringLiteral(format)) =>
                  createExpr(name)(TruncDatePartitionExpr(partColName,
                    format))
                case TruncDate(Cast(
                ExtractBaseColumn(name, TimestampType | StringType), DateType, _, _),
                StringLiteral(format)) =>
                  createExpr(name)(TruncDatePartitionExpr(partColName,
                    format))
                case _ => None
              }
            case other =>
              // Should not happen since we use `Alias` expressions.
              throw DeltaErrors.unexpectedAlias(s"$other")
          }
        case other =>
          // Should not happen since `select` should use `Project`.
          throw DeltaErrors.unexpectedProject(other.toString())
      }
    extractedPartitionExprs.groupBy(_._1).map { case (name, group) =>
      val groupedExprs = group.map(_._2)
      val mergedExprs = mergePartitionExpressionsIfPossible(groupedExprs)
      if (log.isDebugEnabled) {
        logDebug(s"Optimizable partition expressions for column $name:")
        mergedExprs.foreach(expr => logDebug(expr.toString))
      }
      name -> mergedExprs
    }
  }

  /**
   * Merge multiple partition expressions into one if possible. For example, users may define
   * three partitions columns, `year`, `month` and `day`, rather than defining a single `date`
   * partition column. Hence, we need to take the multiple partition columns into a single
   * part to consider when optimizing queries.
   */
  private def mergePartitionExpressionsIfPossible(
      exprs: Seq[OptimizablePartitionExpression]): Seq[OptimizablePartitionExpression] = {
    def isRedundantPartitionExpr(f: OptimizablePartitionExpression): Boolean = {
      f.isInstanceOf[YearPartitionExpr] ||
        f.isInstanceOf[MonthPartitionExpr] ||
        f.isInstanceOf[DayPartitionExpr] ||
        f.isInstanceOf[HourPartitionExpr]
    }

    // Take the first option because it's safe to drop other duplicate partition expressions
    val year = exprs.collect { case y: YearPartitionExpr => y }.headOption
    val month = exprs.collect { case m: MonthPartitionExpr => m }.headOption
    val day = exprs.collect { case d: DayPartitionExpr => d }.headOption
    val hour = exprs.collect { case h: HourPartitionExpr => h }.headOption
    (year ++ month ++ day ++ hour) match {
      case Seq(
          year: YearPartitionExpr,
          month: MonthPartitionExpr,
          day: DayPartitionExpr,
          hour: HourPartitionExpr) =>
        exprs.filterNot(isRedundantPartitionExpr) :+
          YearMonthDayHourPartitionExpr(year.yearPart, month.monthPart, day.dayPart, hour.hourPart)
      case Seq(year: YearPartitionExpr, month: MonthPartitionExpr, day: DayPartitionExpr) =>
        exprs.filterNot(isRedundantPartitionExpr) :+
          YearMonthDayPartitionExpr(year.yearPart, month.monthPart, day.dayPart)
      case Seq(year: YearPartitionExpr, month: MonthPartitionExpr) =>
        exprs.filterNot(isRedundantPartitionExpr) :+
          YearMonthPartitionExpr(year.yearPart, month.monthPart)
      case _ =>
        exprs
    }
  }

  def partitionFilterOptimizationEnabled(spark: SparkSession): Boolean = {
    spark.sessionState.conf
      .getConf(DeltaSQLConf.GENERATED_COLUMN_PARTITION_FILTER_OPTIMIZATION_ENABLED)
  }


  /**
   * Try to generate partition filters from data filters if possible.
   *
   * @param delta the logical plan that outputs the same attributes as the table schema. This will
   *              be used to resolve auto generated expressions.
   */
  def generatePartitionFilters(
      spark: SparkSession,
      snapshot: SnapshotDescriptor,
      dataFilters: Seq[Expression],
      delta: LogicalPlan): Seq[Expression] = {
    if (!satisfyGeneratedColumnProtocol(snapshot.protocol)) {
      return Nil
    }
    if (snapshot.metadata.optimizablePartitionExpressions.isEmpty) {
      return Nil
    }

    val optimizablePartitionExpressions =
      if (spark.sessionState.conf.caseSensitiveAnalysis) {
        snapshot.metadata.optimizablePartitionExpressions
      } else {
        CaseInsensitiveMap(snapshot.metadata.optimizablePartitionExpressions)
      }

    /**
     * Preprocess the data filter such as reordering to ensure the column name appears on the left
     * and the literal appears on the right.
     */
    def preprocess(filter: Expression): Expression = filter match {
      case LessThan(lit: Literal, e: Expression) =>
        GreaterThan(e, lit)
      case LessThanOrEqual(lit: Literal, e: Expression) =>
        GreaterThanOrEqual(e, lit)
      case EqualTo(lit: Literal, e: Expression) =>
        EqualTo(e, lit)
      case GreaterThan(lit: Literal, e: Expression) =>
        LessThan(e, lit)
      case GreaterThanOrEqual(lit: Literal, e: Expression) =>
        LessThanOrEqual(e, lit)
      case e => e
    }

    /**
     * Find the `OptimizablePartitionExpression`s of column `a` and apply them to get the partition
     * filters.
     */
    def toPartitionFilter(
        nameParts: Seq[String],
        func: (OptimizablePartitionExpression) => Option[Expression]): Seq[Expression] = {
      optimizablePartitionExpressions.get(createFieldPath(nameParts)).toSeq.flatMap { exprs =>
        exprs.flatMap(expr => func(expr))
      }
    }

    val partitionFilters = dataFilters.flatMap { filter =>
      preprocess(filter) match {
        case LessThan(ExtractBaseColumn(nameParts, _), lit: Literal) =>
          toPartitionFilter(nameParts, _.lessThan(lit))
        case LessThanOrEqual(ExtractBaseColumn(nameParts, _), lit: Literal) =>
          toPartitionFilter(nameParts, _.lessThanOrEqual(lit))
        case EqualTo(ExtractBaseColumn(nameParts, _), lit: Literal) =>
          toPartitionFilter(nameParts, _.equalTo(lit))
        case GreaterThan(ExtractBaseColumn(nameParts, _), lit: Literal) =>
          toPartitionFilter(nameParts, _.greaterThan(lit))
        case GreaterThanOrEqual(ExtractBaseColumn(nameParts, _), lit: Literal) =>
          toPartitionFilter(nameParts, _.greaterThanOrEqual(lit))
        case IsNull(ExtractBaseColumn(nameParts, _)) =>
          toPartitionFilter(nameParts, _.isNull)
        case _ => Nil
      }
    }

    val resolvedPartitionFilters = resolveReferencesForExpressions(spark, partitionFilters, delta)

    if (log.isDebugEnabled) {
      logDebug("User provided data filters:")
      dataFilters.foreach(f => logDebug(f.sql))
      logDebug("Auto generated partition filters:")
      partitionFilters.foreach(f => logDebug(f.sql))
      logDebug("Resolved generated partition filters:")
      resolvedPartitionFilters.foreach(f => logDebug(f.sql))
    }

    val executionId = Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .getOrElse("unknown")
    recordDeltaEvent(
      snapshot.deltaLog,
      "delta.generatedColumns.optimize",
      data = Map(
        "executionId" -> executionId,
        "triggered" -> resolvedPartitionFilters.nonEmpty
      ))

    resolvedPartitionFilters
  }

  private val DATE_FORMAT_YEAR_MONTH = "yyyy-MM"
  private val DATE_FORMAT_YEAR_MONTH_DAY = "yyyy-MM-dd"
  private val DATE_FORMAT_YEAR_MONTH_DAY_HOUR = "yyyy-MM-dd-HH"
}

/**
 * Finds the full dot-separated path to a field and the data type of the field. This unifies
 * handling of nested and non-nested fields, and allows pattern matching on the data type.
 */
object ExtractBaseColumn {
  def unapply(e: Expression): Option[(Seq[String], DataType)] = e match {
    case AttributeReference(name, dataType, _, _) =>
      Some(Seq(name), dataType)
    case g: GetStructField => g.child match {
      case ExtractBaseColumn(nameParts, _) =>
        Some(nameParts :+ g.extractFieldName, g.dataType)
      case _ => None
    }
    case _ => None
  }
}
