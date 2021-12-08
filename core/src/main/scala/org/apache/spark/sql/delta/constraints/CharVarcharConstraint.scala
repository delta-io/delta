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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.types._

// Delta implements char/varchar length check with CONSTRAINTS, and needs to generate predicate
// expression which is different from the OSS version.
object CharVarcharConstraint {
  final val INVARIANT_NAME = "__CHAR_VARCHAR_STRING_LENGTH_CHECK__"

  def stringConstraints(schema: StructType): Seq[Constraint] = {
    schema.flatMap { f =>
      val targetType = CharVarcharUtils.getRawType(f.metadata).getOrElse(f.dataType)
      val col = UnresolvedAttribute(Seq(f.name))
      checkStringLength(col, targetType).map { lengthCheckExpr =>
        Constraints.Check(INVARIANT_NAME, lengthCheckExpr)
      }
    }
  }

  private def checkStringLength(expr: Expression, dt: DataType): Option[Expression] = dt match {
    case VarcharType(length) =>
      Some(Or(IsNull(expr), LessThanOrEqual(Length(expr), Literal(length))))

    case CharType(length) =>
      checkStringLength(expr, VarcharType(length))

    case StructType(fields) =>
      fields.zipWithIndex.flatMap { case (f, i) =>
        checkStringLength(GetStructField(expr, i, Some(f.name)), f.dataType)
      }.reduceOption(And(_, _))

    case ArrayType(et, containsNull) =>
      checkStringLengthInArray(expr, et, containsNull)

    case MapType(kt, vt, valueContainsNull) =>
      (checkStringLengthInArray(MapKeys(expr), kt, false) ++
        checkStringLengthInArray(MapValues(expr), vt, valueContainsNull))
        .reduceOption(And(_, _))

    case _ => None
  }

  private def checkStringLengthInArray(
      arr: Expression, et: DataType, containsNull: Boolean): Option[Expression] = {
    val cleanedType = CharVarcharUtils.replaceCharVarcharWithString(et)
    val param = NamedLambdaVariable("x", cleanedType, containsNull)
    checkStringLength(param, et).map { checkExpr =>
      Or(IsNull(arr), ArrayForAll(arr, LambdaFunction(checkExpr, Seq(param))))
    }
  }
}
