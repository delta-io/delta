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
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.DeltaDataSource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistryBase, NamedRelation, TableFunctionRegistry, UnresolvedLeafNode, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExpressionInfo, StringLiteral}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Resolve Delta specific table-value functions.
 */
object DeltaTableValueFunctions {
  val CDC_NAME_BASED = "table_changes"
  val CDC_PATH_BASED = "table_changes_by_path"
  val supportedFnNames = Seq(CDC_NAME_BASED, CDC_PATH_BASED)

  // For use with SparkSessionExtensions
  type TableFunctionDescription =
    (FunctionIdentifier, ExpressionInfo, TableFunctionRegistry.TableFunctionBuilder)

  /**
   * For a supported Delta table value function name, get the TableFunctionDescription to be
   * injected in DeltaSparkSessionExtension
   */
  def getTableValueFunctionInjection(fnName: String): TableFunctionDescription = {
    val (info, builder) = fnName match {
      case CDC_NAME_BASED => FunctionRegistryBase.build[CDCNameBased](fnName, since = None)
      case CDC_PATH_BASED => FunctionRegistryBase.build[CDCPathBased](fnName, since = None)
      case _ => throw DeltaErrors.invalidTableValueFunction(fnName)
    }
    val ident = FunctionIdentifier(fnName)
    (ident, info, builder)
  }
}

///////////////////////////////////////////////////////////////////////////
//                     Logical plans for Delta TVFs                      //
///////////////////////////////////////////////////////////////////////////

/**
 * Represents an unresolved Delta Table Value Function
 */
trait DeltaTableValueFunction extends UnresolvedLeafNode {
  def fnName: String
  val functionArgs: Seq[Expression]
}

/**
 * Base trait for analyzing `table_changes` and `table_changes_for_path`. The resolution works as
 * follows:
 *  1. The TVF logical plan is resolved using the TableFunctionRegistry in the Analyzer. This uses
 *     reflection to create one of `CDCNameBased` or `CDCPathBased` by passing all the arguments.
 *  2. DeltaAnalysis turns the plans to a `TableChanges` node to resolve the DeltaTable. This can
 *     be resolved by the DeltaCatalog for tables or DeltaAnalysis for the path based use.
 *  3. TableChanges then turns into a LogicalRelation that returns the CDC relation.
 */
trait CDCStatementBase extends DeltaTableValueFunction {
  /** Get the table that the function is being called on as an unresolved relation */
  protected def getTable(spark: SparkSession, name: Expression): LogicalPlan

  if (functionArgs.size < 2) {
    throw new DeltaAnalysisException(
      errorClass = "INCORRECT_NUMBER_OF_ARGUMENTS",
      messageParameters = Array(
        "not enough args", // failure
        fnName,
        "2", // minArgs
        "3")) // maxArgs
  }
  if (functionArgs.size > 3) {
    throw new DeltaAnalysisException(
      errorClass = "INCORRECT_NUMBER_OF_ARGUMENTS",
      messageParameters = Array(
        "too many args", // failure
        fnName,
        "2", // minArgs
        "3")) // maxArgs
  }

  protected def getOptions: CaseInsensitiveStringMap = {
    def toDeltaOption(keyPrefix: String, value: Expression): (String, String) = {
      val evaluated = DeltaTableValueFunctionsShims.evaluateTimeOption(value)
      value.dataType match {
        // We dont need to explicitly handle ShortType as it is parsed as IntegerType.
        case _: IntegerType | LongType => (keyPrefix + "Version") -> evaluated
        case _: StringType => (keyPrefix + "Timestamp") -> evaluated
        case _: TimestampType => (keyPrefix + "Timestamp") -> {
          val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          // when evaluated the time is represented with microseconds, which needs to be trimmed.
          fmt.format(new Date(evaluated.toLong / 1000))
        }
        case _ =>
          throw DeltaErrors.unsupportedExpression(s"${keyPrefix} option", value.dataType,
            Seq("IntegerType", "LongType", "StringType", "TimestampType"))
      }
    }

    val startingOption = toDeltaOption("starting", functionArgs(1))
    val endingOption = functionArgs.drop(2).headOption.map(toDeltaOption("ending", _))
    val options = Map(DeltaDataSource.CDC_ENABLED_KEY -> "true", startingOption) ++ endingOption
    new CaseInsensitiveStringMap(options.asJava)
  }

  protected def getStringLiteral(e: Expression, whatFor: String): String = e match {
    case StringLiteral(value) => value
    case o =>
      throw DeltaErrors.unsupportedExpression(whatFor, o.dataType, Seq("StringType literal"))
  }

  def toTableChanges(spark: SparkSession): TableChanges =
    TableChanges(getTable(spark, functionArgs.head), fnName)
}

/**
 * Plan for the "table_changes" function
 */
case class CDCNameBased(override val functionArgs: Seq[Expression])
  extends CDCStatementBase {
  override def fnName: String = DeltaTableValueFunctions.CDC_NAME_BASED
  // Provide a constructor to get a better error message, when no expressions are provided
  def this() = this(Nil)

  override protected def getTable(spark: SparkSession, name: Expression): LogicalPlan = {
    val stringId = getStringLiteral(name, "table name")
    val identifier = spark.sessionState.sqlParser.parseMultipartIdentifier(stringId)
    UnresolvedRelation(identifier, getOptions, isStreaming = false)
  }
}

/**
 * Plan for the "table_changes_by_path" function
 */
case class CDCPathBased(override val functionArgs: Seq[Expression])
  extends CDCStatementBase {
  override def fnName: String = DeltaTableValueFunctions.CDC_PATH_BASED
  // Provide a constructor to get a better error message, when no expressions are provided
  def this() = this(Nil)

  override protected def getTable(spark: SparkSession, name: Expression): LogicalPlan = {
    UnresolvedPathBasedDeltaTableRelation(getStringLiteral(name, "table path"), getOptions)
  }
}

case class TableChanges(
    child: LogicalPlan,
    fnName: String,
    cdcAttr: Seq[Attribute] = CDCReader.cdcAttributes) extends UnaryNode {

  override lazy val resolved: Boolean = false
  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    this.copy(child = newChild)

  override def output: Seq[Attribute] = Nil

  /** Converts the table changes plan to a query over a Delta table */
  def toReadQuery: LogicalPlan = child.transformUp {
    case DataSourceV2Relation(d: DeltaTableV2, _, _, _, options) =>
      // withOptions empties the catalog table stats
      d.withOptions(options.asScala.toMap).toLogicalRelation
    case r: NamedRelation =>
      throw DeltaErrors.notADeltaTableException(fnName, r.name)
    case l: LogicalRelation =>
      val relationName = l.catalogTable.map(_.identifier.toString).getOrElse("relation")
      throw DeltaErrors.notADeltaTableException(fnName, relationName)
  }
}
