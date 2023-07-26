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
import java.util.{Date, Locale}

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistryBase, TableFunctionRegistry, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
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

  /**
   * Resolution for CDC read table valued functions. Currently we support two apis.
   *      1. table_changes for CDC reads on metastore tables.
   *      2. table_changes_by_path for CDC reads on path based tables.
   */
  private[delta] def resolveChangesTableValueFunctions(
    session: SparkSession, fnName: String, args: Seq[Expression]): LogicalPlan = {

    if (args.size < 2) {
      throw new DeltaAnalysisException(
        errorClass = "INCORRECT_NUMBER_OF_ARGUMENTS",
        messageParameters = Array(
          "not enough args", // failure
          fnName, // functionName
          "2", // minArgs
          "3")) // maxArgs
    }
    if (args.size > 3) {
      throw new DeltaAnalysisException(
        errorClass = "INCORRECT_NUMBER_OF_ARGUMENTS",
        messageParameters = Array(
          "too many args", // failure
          fnName, // functionName
          "2", // minArgs
          "3")) // maxArgs
    }

    val tableNameExpr = args.head

    def toDeltaOption(keyPrefix: String, value: Expression): (String, String) = {
      value.dataType match {
        // We dont need to explicitly handle ShortType as it is parsed as IntegerType.
        case _: IntegerType | LongType => (keyPrefix + "Version") -> value.eval().toString
        case _: StringType => (keyPrefix + "Timestamp") -> value.eval().toString
        case _: TimestampType => (keyPrefix + "Timestamp") -> {
          val time = value.eval().toString
          val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          // when evaluated the time is represented with microseconds, which needs to be trimmed.
          fmt.format(new Date(time.toLong / 1000))
        }
        case _ =>
          throw DeltaErrors.unsupportedExpression(s"${keyPrefix} option", value.dataType,
            Seq("IntegerType", "LongType", "StringType", "TimestampType"))
      }
    }

    val startingOption = toDeltaOption("starting", args(1))
    val endingOption = args.drop(2).headOption.map(toDeltaOption("ending", _))
    val options = Map(DeltaDataSource.CDC_ENABLED_KEY -> "true", startingOption) ++ endingOption

    val table = if (fnName.toLowerCase(Locale.ROOT) == CDC_NAME_BASED) {
      val tableId: TableIdentifier = tableNameExpr match {
        case l: Literal if l.dataType == StringType =>
          session.sessionState.sqlParser.parseTableIdentifier(tableNameExpr.eval().toString)
        case _ =>
          throw DeltaErrors.unsupportedExpression(
            "table name", tableNameExpr.dataType, Seq("Literal of type StringType"))
      }
      val catalogTable = session.sessionState.catalog.getTableMetadata(tableId)
      DeltaTableV2(
        session,
        path = new Path(catalogTable.location),
        catalogTable = Some(catalogTable),
        tableIdentifier = Some(tableId.unquotedString),
        timeTravelOpt = None,
        options = Map.empty,
        cdcOptions = new CaseInsensitiveStringMap(options.asJava))
    } else if (fnName.toLowerCase(Locale.ROOT) == CDC_PATH_BASED) {
      val path = tableNameExpr match {
        case _: Literal  if tableNameExpr.dataType == StringType =>
          tableNameExpr.eval().toString
        case _ =>
          throw DeltaErrors.unsupportedExpression(
            "table path", tableNameExpr.dataType, Seq("StringType"))
      }
      DeltaTableV2(
        session,
        path = new Path(path),
        catalogTable = None,
        tableIdentifier = None,
        timeTravelOpt = None,
        options = Map.empty,
        cdcOptions = new CaseInsensitiveStringMap(options.asJava))
    } else {
      throw DeltaErrors.invalidTableValueFunction(fnName)
    }
        val relation = table.toBaseRelation
        LogicalRelation(
          relation,
          relation.schema.toAttributes,
          // time traveled relations shouldn't pass catalog stats as stats may be incorrect
          table.catalogTable.map(_.copy(stats = None)),
          isStreaming = false)
  }

}

///////////////////////////////////////////////////////////////////////////
//                     Logical plans for Delta TVFs                      //
///////////////////////////////////////////////////////////////////////////

/**
 * Represents an unresolved Delta Table Value Function
 *
 * @param fnName  can be one of [[DeltaTableValueFunctions.supportedFnNames]].
 */
abstract class DeltaTableValueFunction(val fnName: String) extends LeafNode {
  override def output: Seq[Attribute] = Nil
  override lazy val resolved = false

  val functionArgs: Seq[Expression]
}

/**
 * Plan for the "table_changes" function
 */
case class CDCNameBased(override val functionArgs: Seq[Expression])
  extends DeltaTableValueFunction(DeltaTableValueFunctions.CDC_NAME_BASED)

/**
 * Plan for the "table_changes_by_path" function
 */
case class CDCPathBased(override val functionArgs: Seq[Expression])
  extends DeltaTableValueFunction(DeltaTableValueFunctions.CDC_PATH_BASED)
