/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.delta.constraints.{Constraint, Constraints}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSourceUtils.GENERATION_EXPRESSION_METADATA_KEY

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchFunctionException, UnresolvedFunction}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.IncrementalExecution
import org.apache.spark.sql.types.{LongType, Metadata => FieldMetadata, MetadataBuilder, StructField, StructType}

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
object GeneratedColumn extends DeltaLogging {

  def isGeneratedColumn(attr: Attribute): Boolean = {
    attr.metadata.contains(GENERATION_EXPRESSION_METADATA_KEY)
  }

  def isGeneratedColumn(field: StructField): Boolean = {
    field.metadata.contains(GENERATION_EXPRESSION_METADATA_KEY)
  }

  /**  Whether exists any generated columns in the schema. */
  def hasGeneratedColumns(schema: StructType): Boolean = {
    schema.exists(isGeneratedColumn)
  }

  /** Return the generation expression from a field metadata if any. */
  def getGenerationExpressionStr(metadata: FieldMetadata): Option[String] = {
    if (metadata.contains(GENERATION_EXPRESSION_METADATA_KEY)) {
      Some(metadata.getString(GENERATION_EXPRESSION_METADATA_KEY))
    } else {
      None
    }
  }

  /** Return the generation expression from a field if any. */
  def getGenerationExpressionStr(field: StructField): Option[String] = {
    getGenerationExpressionStr(field.metadata)
  }

  /** Create a new field with the given generation expression. */
  def withGenerationExpression(field: StructField, expr: String): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(field.metadata)
      .putString(GENERATION_EXPRESSION_METADATA_KEY, expr)
      .build()
    field.copy(metadata = newMetadata)
  }

  /** Parse a generation expression string and convert it to an [[Expression]] object. */
  def parseGenerationExpression(spark: SparkSession, exprString: String): Expression = {
    val parsedExpr = spark.sessionState.sqlParser.parseExpression(exprString)
    replacePartitionTransformExprWithStandardExpr(spark, parsedExpr)
  }

  /**
   * As partition transform expressions are not built-in functions and cannot be used in a regular
   * SQL expression, we need to replace them with alternative SQL standard expression.
   */
  private def replacePartitionTransformExprWithStandardExpr(
      spark: SparkSession,
      expr: Expression): Expression = {
    val conf = spark.sessionState.conf
    val catalog = spark.sessionState.catalog

    // This function is copied from Analyzer.LookupFunctions.normalizeFuncName
    def normalizeFuncName(name: FunctionIdentifier): FunctionIdentifier = {
      val funcName = if (conf.caseSensitiveAnalysis) {
        name.funcName
      } else {
        name.funcName.toLowerCase(Locale.ROOT)
      }

      val databaseName = name.database match {
        case Some(a) => formatDatabaseName(a)
        case None => catalog.getCurrentDatabase
      }

      FunctionIdentifier(funcName, Some(databaseName))
    }

    // This function is copied from Analyzer.LookupFunctions.normalizeFuncName
    def formatDatabaseName(name: String): String = {
      if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
    }

    // The following transform logic is similar to `Analyzer.LookupFunctions` except we need to
    // recognize partition transform expressions and convert them to normal SQL expressions.
    val externalFunctionNameSet = new mutable.HashSet[FunctionIdentifier]()
    expr.transform {
      case f: UnresolvedFunction
        if externalFunctionNameSet.contains(normalizeFuncName(f.name)) => f
      case f: UnresolvedFunction if catalog.isRegisteredFunction(f.name) => f
      case f: UnresolvedFunction if catalog.isPersistentFunction(f.name) =>
        externalFunctionNameSet.add(normalizeFuncName(f.name))
        f
      case f: UnresolvedFunction =>
        val checkPartitionTransformExpression =
          f.name.database.isEmpty

        if (checkPartitionTransformExpression) {
          import org.apache.spark.sql.catalyst.dsl.expressions._

          // Replace the partition transform expressions to normal SQL expressions. We are doing
          // this manually rather than registering them as UDFs because registering them as UDFs
          // may break code not using generated columns (e.g., a user may register their own
          // UDF with the same name). Note: the function name is always case insensitive.
          f.name.funcName.toLowerCase(Locale.ROOT) match {
            case "years" =>
              if (f.arguments.length != 1) {
                throw DeltaErrors.partitionTransformExpressionNotEnoughParameter(
                  "years",
                  numOfExpectedParameters = 1,
                  numOfActualParameters = f.arguments.length)
              }
              // The expression returns the year component of the date/timestamp as an integer. For
              // example, an integer 2021.
              Year(f.arguments(0)).cast(LongType)
            case "months" =>
              if (f.arguments.length != 1) {
                throw DeltaErrors.partitionTransformExpressionNotEnoughParameter(
                  "months",
                  numOfExpectedParameters = 1,
                  numOfActualParameters = f.arguments.length)
              }
              // The expression returns the year and month component of the date/timestamp as an
              // integer. For example, an integer 202102.
              Year(f.arguments(0)) * 100L + Month(f.arguments(0))
            case "days" =>
              if (f.arguments.length != 1) {
                throw DeltaErrors.partitionTransformExpressionNotEnoughParameter(
                  "days",
                  numOfExpectedParameters = 1,
                  numOfActualParameters = f.arguments.length)
              }
              // The expression returns the year, month and day components of the date/timestamp as
              // an integer. For example, an integer 20210212.
              Year(f.arguments(0)) * 10000L + Month(f.arguments(0)) * 100L +
                DayOfMonth(f.arguments(0))
            case "hours" =>
              if (f.arguments.length != 1) {
                throw DeltaErrors.partitionTransformExpressionNotEnoughParameter(
                  "hours",
                  numOfExpectedParameters = 1,
                  numOfActualParameters = f.arguments.length)
              }
              // The expression returns the year, month, day and hour components of the
              // date/timestamp as an integer. For example, an integer 2021021205.
              Year(f.arguments(0)) * 1000000L + Month(f.arguments(0)) * 10000L +
                DayOfMonth(f.arguments(0)) * 100L + Hour(f.arguments(0))
            case "bucket" =>
              throw DeltaErrors.operationNotSupportedException("bucket")
            case _ =>
              throw new NoSuchFunctionException(
                f.name.database.getOrElse(catalog.getCurrentDatabase),
                f.name.funcName)
          }
        } else {
          throw new NoSuchFunctionException(
            f.name.database.getOrElse(catalog.getCurrentDatabase),
            f.name.funcName)
        }
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
    val df = Dataset.ofRows(spark, new LocalRelation(StructType(normalColumns).toAttributes))
    val selectExprs = generatedColumns.map { f =>
      getGenerationExpressionStr(f) match {
        case Some(exprString) =>
          val expr = parseGenerationExpression(df.sparkSession, exprString)
          new Column(expr).alias(f.name)
        case None =>
          // Should not happen
          throw new IllegalStateException(
            s"Cannot find the expressions in the generated column ${f.name}")
      }
    }
    val dfWithExprs = try {
      df.select(selectExprs: _*)
    } catch {
      case e: AnalysisException if e.getMessage != null =>
        // Improve the column resolution error
        "cannot resolve.*?given input columns:.*?".r.findFirstMatchIn(e.getMessage) match {
          case Some(_) => throw DeltaErrors.generatedColumnsReferToWrongColumns(e)
          case None => throw e
        }
    }
    // Check whether the generation expressions are valid
    dfWithExprs.queryExecution.analyzed.transformAllExpressions {
      case expr: Alias =>
        // Alias will be non deterministic if it points to a non deterministic expression.
        // Skip `Alias` to provide a better error for a non deterministic expression.
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

  /**
   * If there are generated columns in `schema`, add a new project to generate the generated columns
   * missing in the schema, and return constraints for generated columns existing in the schema.
   */
  def addGeneratedColumnsOrReturnConstraints(
      deltaLog: DeltaLog,
      queryExecution: QueryExecution,
      schema: StructType,
      df: DataFrame): (DataFrame, Seq[Constraint]) = {
    val topLevelOutputNames = CaseInsensitiveMap(df.schema.map(f => f.name -> f).toMap)
    val constraints = mutable.ArrayBuffer[Constraint]()
    val selectExprs = schema.map { f =>
      getGenerationExpressionStr(f) match {
        case Some(exprString) =>
          val expr = parseGenerationExpression(df.sparkSession, exprString)
          if (topLevelOutputNames.contains(f.name)) {
            val column = SchemaUtils.fieldToColumn(f)
            constraints +=
              Constraints.Check("Auto Generated Column", EqualTo(column.expr, expr))
            column.alias(f.name)
          } else {
            new Column(expr).alias(f.name)
          }
        case None =>
          SchemaUtils.fieldToColumn(f).alias(f.name)
      }
    }
    val newData = queryExecution match {
      case incrementalExecution: IncrementalExecution =>
        selectFromStreamingDataFrame(incrementalExecution, df, selectExprs: _*)
      case _ => df.select(selectExprs: _*)
    }
    recordDeltaEvent(deltaLog, "delta.generatedColumns.write")
    (newData, constraints)
  }

  /**
   * Select `cols` from a micro batch DataFrame. Directly calling `select` won't work because it
   * will create a `QueryExecution` rather than inheriting `IncrementalExecution` from
   * the micro batch DataFrame. A streaming micro batch DataFrame to execute should use
   * `IncrementalExecution`.
   */
  def selectFromStreamingDataFrame(
      incrementalExecution: IncrementalExecution,
      df: DataFrame,
      cols: Column*): DataFrame = {
    val newMicroBatch = df.select(cols: _*)
    val newIncrementalExecution = new IncrementalExecution(
      newMicroBatch.sparkSession,
      newMicroBatch.queryExecution.logical,
      incrementalExecution.outputMode,
      incrementalExecution.checkpointLocation,
      incrementalExecution.queryId,
      incrementalExecution.runId,
      incrementalExecution.currentBatchId,
      incrementalExecution.offsetSeqMetadata
    )
    newIncrementalExecution.executedPlan // Force the lazy generation of execution plan
    // Use reflection to call the private constructor.
    val constructor =
      classOf[Dataset[_]].getConstructor(classOf[QueryExecution], classOf[Encoder[_]])
    constructor.newInstance(
      newIncrementalExecution,
      RowEncoder(newIncrementalExecution.analyzed.schema)).asInstanceOf[DataFrame]
  }
}
