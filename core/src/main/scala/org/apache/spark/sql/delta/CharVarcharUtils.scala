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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// TODO: remove this file from Delta lake when Spark 3.1 is released.
object CharVarcharUtils extends Logging {

  private val CHAR_VARCHAR_TYPE_STRING_METADATA_KEY = "__CHAR_VARCHAR_TYPE_STRING"

  /**
   * Replaces CharType/VarcharType with StringType recursively in the given struct type. If a
   * top-level StructField's data type is CharType/VarcharType or has nested CharType/VarcharType,
   * this method will add the original type string to the StructField's metadata, so that we can
   * re-construct the original data type with CharType/VarcharType later when needed.
   */
  def replaceCharVarcharWithStringInSchema(st: StructType): StructType = {
    StructType(st.map { field =>
      if (hasCharVarchar(field.dataType)) {
        val metadata = new MetadataBuilder().withMetadata(field.metadata)
          .putString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY, field.dataType.catalogString).build()
        field.copy(dataType = replaceCharVarcharWithString(field.dataType), metadata = metadata)
      } else {
        field
      }
    })
  }

  /**
   * Returns true if the given data type is CharType/VarcharType or has nested CharType/VarcharType.
   */
  def hasCharVarchar(dt: DataType): Boolean = dt match {
    case s: StructType => s.fields.exists(f => hasCharVarchar(f.dataType))
    case a: ArrayType => hasCharVarchar(a.elementType)
    case m: MapType => hasCharVarchar(m.keyType) || hasCharVarchar(m.valueType)
    case _: CharType => true
    case _: VarcharType => true
    case _ => false
  }

  private def charVarcharAsString: Boolean = {
    SQLConf.get.getConfString("spark.sql.legacy.charVarcharAsString", "false").toBoolean
  }

  /**
   * Validate the given [[DataType]] to fail if it is char or varchar types or contains nested ones
   */
  def failIfHasCharVarchar(dt: DataType): DataType = {
    if (!charVarcharAsString && hasCharVarchar(dt)) {
      throw new AnalysisException("char/varchar type can only be used in the table schema. " +
        s"You can set spark.sql.legacy.charVarcharAsString to true, so that Spark" +
        s" treat them as string type as same as Spark 3.0 and earlier")
    } else {
      replaceCharVarcharWithString(dt)
    }
  }

  /**
   * Replaces CharType/VarcharType with StringType recursively in the given data type.
   */
  def replaceCharVarcharWithString(dt: DataType): DataType = dt match {
    case ArrayType(et, nullable) =>
      ArrayType(replaceCharVarcharWithString(et), nullable)
    case MapType(kt, vt, nullable) =>
      MapType(replaceCharVarcharWithString(kt), replaceCharVarcharWithString(vt), nullable)
    case StructType(fields) =>
      StructType(fields.map { field =>
        field.copy(dataType = replaceCharVarcharWithString(field.dataType))
      })
    case _: CharType => StringType
    case _: VarcharType => StringType
    case _ => dt
  }

  /**
   * Removes the metadata entry that contains the original type string of CharType/VarcharType from
   * the given attribute's metadata.
   */
  def cleanAttrMetadata(attr: AttributeReference): AttributeReference = {
    val cleaned = new MetadataBuilder().withMetadata(attr.metadata)
      .remove(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY).build()
    attr.withMetadata(cleaned)
  }

  /**
   * Re-construct the original data type from the type string in the given metadata.
   * This is needed when dealing with char/varchar columns/fields.
   */
  def getRawType(metadata: Metadata): Option[DataType] = {
    if (metadata.contains(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY)) {
      Some(CatalystSqlParser.parseDataType(
        metadata.getString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY)))
    } else {
      None
    }
  }

  /**
   * Returns an expression to apply write-side string length check for the given expression. A
   * string value can not exceed N characters if it's written into a CHAR(N)/VARCHAR(N)
   * column/field.
   */
  def stringLengthCheck(expr: Expression, targetAttr: Attribute): Expression = {
    getRawType(targetAttr.metadata).map { rawType =>
      stringLengthCheck(expr, rawType)
    }.getOrElse(expr)
  }

  private def stringLengthCheck(expr: Expression, dt: DataType): Expression = {
    dt match {
      case CharType(length) =>
        StaticInvoke(
          CharVarcharCodegenUtils.getClass,
          StringType,
          "charTypeWriteSideCheck",
          expr :: Literal(length) :: Nil,
          returnNullable = false)

      case VarcharType(length) =>
        StaticInvoke(
          CharVarcharCodegenUtils.getClass,
          StringType,
          "varcharTypeWriteSideCheck",
          expr :: Literal(length) :: Nil,
          returnNullable = false)

      case StructType(fields) =>
        val struct = CreateNamedStruct(fields.zipWithIndex.flatMap { case (f, i) =>
          Seq(Literal(f.name),
            stringLengthCheck(GetStructField(expr, i, Some(f.name)), f.dataType))
        })
        if (expr.nullable) {
          If(IsNull(expr), Literal(null, struct.dataType), struct)
        } else {
          struct
        }

      case ArrayType(et, containsNull) => stringLengthCheckInArray(expr, et, containsNull)

      case MapType(kt, vt, valueContainsNull) =>
        val newKeys = stringLengthCheckInArray(MapKeys(expr), kt, containsNull = false)
        val newValues = stringLengthCheckInArray(MapValues(expr), vt, valueContainsNull)
        MapFromArrays(newKeys, newValues)

      case _ => expr
    }
  }

  private def stringLengthCheckInArray(
      arr: Expression, et: DataType, containsNull: Boolean): Expression = {
    val param = NamedLambdaVariable("x", replaceCharVarcharWithString(et), containsNull)
    val func = LambdaFunction(stringLengthCheck(param, et), Seq(param))
    ArrayTransform(arr, func)
  }
}

object CharVarcharCodegenUtils {
  private val SPACE = UTF8String.fromString(" ")

  private def trimTrailingSpaces(inputStr: UTF8String, numChars: Int, limit: Int) = {
    if (inputStr.trimRight.numChars > limit) {
      throw new RuntimeException("Exceeds char/varchar type length limitation: " + limit)
    }
    inputStr.substring(0, limit)
  }

  def charTypeWriteSideCheck(inputStr: UTF8String, limit: Int): UTF8String = {
    val numChars = inputStr.numChars
    if (numChars == limit) inputStr
    else if (numChars < limit) inputStr.rpad(limit, SPACE)
    else trimTrailingSpaces(inputStr, numChars, limit)
  }

  def varcharTypeWriteSideCheck(inputStr: UTF8String, limit: Int): UTF8String = {
    val numChars = inputStr.numChars
    if (numChars <= limit) inputStr
    else trimTrailingSpaces(inputStr, numChars, limit)
  }
}
