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

package org.apache.spark.sql.delta.constraints

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * List of invariants that can be defined on a Delta table that will allow us to perform
 * validation checks during changes to the table.
 */
object Invariants {
  sealed trait Rule {
    val name: String
  }

  /** Used for columns that should never be null. */
  case class NotNull(fieldPath: Seq[String], expression: Expression) extends Rule {
    override val name: String = s"${fieldPath.mkString(".")} NOT NULL"

    def mapExpr(f: Expression => Expression): NotNull = {
      copy(expression = f(expression))
    }
  }

  sealed trait RulePersistedInMetadata {
    def wrap: PersistedRule
    def json: String = JsonUtils.toJson(wrap)
  }

  /** Rules that are persisted in the metadata field of a schema. */
  case class PersistedRule(expression: PersistedExpression = null) {
    def unwrap: RulePersistedInMetadata = {
      if (expression != null) {
        expression
      } else {
        null
      }
    }
  }

  /** A SQL expression to check for when writing out data. */
  case class ArbitraryExpression(expression: Expression) extends Rule {
    override val name: String = s"EXPRESSION($expression)"
  }

  object ArbitraryExpression {
    def apply(sparkSession: SparkSession, exprString: String): ArbitraryExpression = {
      val expr = sparkSession.sessionState.sqlParser.parseExpression(exprString)
      ArbitraryExpression(expr)
    }
  }

  /** Persisted companion of the ArbitraryExpression rule. */
  case class PersistedExpression(expression: String) extends RulePersistedInMetadata {
    override def wrap: PersistedRule = PersistedRule(expression = this)
  }

  /**
   * Find all non-nullable fields in the schema, creating expressions for each that checks if
   * any nullable parent is null or the final field is not null.
   *
   * Arrays are checked using an ArrayForall expression to check each element in the list.
   * Maps are checked an ArrayForall on the keys and the values.
   *
   * @param dataType the data type of the current field
   * @param fieldPath the path to the current field
   * @param parentExtraction the expression to extract the current field
   * @return
   */
  private def traverseNullCheckExpressions(
      dataType: DataType,
      fieldPath: Seq[String],
      parentExtraction: Option[Expression] = None
  ): Seq[NotNull] = {
    val nullCheckExpressions = new ArrayBuffer[NotNull]

    dataType match {
      case StructType(fields) => fields.map { field =>
        val childFieldPath = fieldPath :+ field.name

        // If it's a top level field, we create a new attribute, otherwise get a nested struct
        // field
        val fieldExtraction = parentExtraction.map(UnresolvedExtractValue(_, Literal(field.name)))
          .getOrElse(UnresolvedAttribute(Seq(field.name)))

        var childNullCheckExpressions = traverseNullCheckExpressions(
          field.dataType,
          childFieldPath,
          Some(fieldExtraction))

        if (!field.nullable) {
          // If the field is non-nullable, we return a check for this field not being null
          // or any of it's nullable parents being null
          nullCheckExpressions +=
            NotNull(childFieldPath, IsNotNull(fieldExtraction))
        } else {
          // If the field is nullable, we include the null check of the field as an allowable
          // condition
          childNullCheckExpressions = childNullCheckExpressions.map { n =>
            n.mapExpr(Or(IsNull(fieldExtraction), _))
          }
        }

        nullCheckExpressions ++= childNullCheckExpressions
      }
      case ArrayType(elementType, containsNull) =>
        val childFieldPath = fieldPath :+ "element"
        val elementVar = UnresolvedNamedLambdaVariable(Seq("elementVar"))
        val lambdaCheckExpressions = ArrayBuffer.empty[Expression]

        // Traverse into the child type in case it's a complex type and include it's checks
        // on the lambda variable
        var childNullCheckExpressions = traverseNullCheckExpressions(
          elementType,
          childFieldPath,
          Some(elementVar)
        )

        if (!containsNull) {
          nullCheckExpressions += NotNull(childFieldPath, ArrayForAll(parentExtraction.get,
            LambdaFunction(IsNotNull(elementVar), Seq(elementVar))))
        } else {
          childNullCheckExpressions = childNullCheckExpressions.map { n =>
            n.mapExpr(Or(IsNull(elementVar), _))
          }
        }

        nullCheckExpressions ++= childNullCheckExpressions.map { n =>
          n.mapExpr { lambdaExpression =>
            ArrayForAll(parentExtraction.get, LambdaFunction(lambdaExpression, Seq(elementVar)))
          }
        }
      case MapType(keyType, valueType, valueContainsNull) =>
        val keyFieldPath = fieldPath :+ "key"
        val keyVar = UnresolvedNamedLambdaVariable(Seq("keyVar"))
        val keyCheckExpressions = ArrayBuffer.empty[Expression]

        val valueFieldPath = fieldPath :+ "value"
        val valueVar = UnresolvedNamedLambdaVariable(Seq("valueVar"))
        val valueCheckExpressions = ArrayBuffer.empty[Expression]

        // Traverse into the key and value types in case it's a complex type and include it's checks
        // on the lambda variable
        var keyNullCheckExpressions = traverseNullCheckExpressions(
          keyType,
          keyFieldPath,
          Some(keyVar)
        )
        var valueNullCheckExpressions = traverseNullCheckExpressions(
          valueType,
          valueFieldPath,
          Some(valueVar)
        )

        if (!valueContainsNull) {
          // If the value is non-nullable, add a check that each element is not null
          nullCheckExpressions += NotNull(valueFieldPath,
            ArrayForAll(MapValues(parentExtraction.get),
            LambdaFunction(IsNotNull(valueVar), Seq(valueVar))))
        } else {
          valueNullCheckExpressions = valueNullCheckExpressions.map { n =>
            n.mapExpr(Or(IsNull(valueVar), _))
          }
        }

        nullCheckExpressions ++= keyNullCheckExpressions.map { n =>
          n.mapExpr { lambdaExpression =>
            ArrayForAll(MapKeys(parentExtraction.get),
              LambdaFunction(lambdaExpression, Seq(keyVar)))
          }
        }

        nullCheckExpressions ++= valueNullCheckExpressions.map { n =>
          n.mapExpr { lambdaExpression =>
            ArrayForAll(MapValues(parentExtraction.get),
              LambdaFunction(lambdaExpression, Seq(valueVar)))
          }
        }

      case _ => ()
    }

    nullCheckExpressions.toSeq
  }

  /** Extract invariants from the given schema */
  def getFromSchema(schema: StructType, spark: SparkSession): Seq[Constraint] = {
    val nullConstraints = traverseNullCheckExpressions(schema, Seq.empty).map {
      case NotNull(fieldPath, expr) => Constraints.NotNull(fieldPath, expr)
    }

    val checkConstraints = SchemaUtils.filterRecursively(schema,
        checkComplexTypes = false) { field =>
      field.metadata.contains(INVARIANTS_FIELD)
    }.map { case (parents, field) =>
      val rule = field.metadata.getString(INVARIANTS_FIELD)
      val invariant = Option(JsonUtils.mapper.readValue[PersistedRule](rule).unwrap) match {
        case Some(PersistedExpression(exprString)) =>
          ArbitraryExpression(spark, exprString)
        case _ =>
          throw DeltaErrors.unrecognizedInvariant()
      }
      Constraints.Check(invariant.name, invariant.expression)
    }

    nullConstraints ++ checkConstraints
  }

  val INVARIANTS_FIELD = "delta.invariants"
}

/** A rule applied on a column to ensure data hygiene. */
case class Invariant(column: Seq[String], rule: Invariants.Rule)
