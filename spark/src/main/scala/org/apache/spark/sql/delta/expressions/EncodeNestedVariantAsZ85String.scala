/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.delta.util.DeltaStatsJsonUtils
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.Variant
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

/**
 * An expression that encodes VariantVal fields in a struct as Z85 strings.
 *
 * When converting stats structs to JSON for state reconstruction, variants need to be
 * encoded as Z85 strings to preserve their binary representation. This expression walks
 * through the struct and replaces any VariantVal fields with their Z85 string encoding.
 *
 * The output schema has VariantType fields replaced with StringType.
 *
 * @param child The expression producing the row with VariantVal fields.
 */
case class EncodeNestedVariantAsZ85String(child: Expression)
  extends UnaryExpression with CodegenFallback {

  override def dataType: DataType = transformDataType(child.dataType)

  override def nullable: Boolean = child.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!child.dataType.isInstanceOf[StructType]) {
      TypeCheckResult.TypeCheckFailure(s"The top-level data type for the input to " +
        s"EncodeNestedVariantAsZ85String must be StructType but this is not true " +
        s"in: ${child.dataType}.")
    } else if (!isValidType(child.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        s"EncodeNestedVariantAsZ85String does not support arrays or maps in schema. " +
        s"Found: ${child.dataType}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  // The data type cannot contain arrays or maps since stats structs do not have arrays or maps yet.
  private def isValidType(dataType: DataType): Boolean = {
    dataType match {
      case _: ArrayType => false
      case _: MapType => false
      case st: StructType =>
        st.fields.forall(field => isValidType(field.dataType))
      case _ => true
    }
  }

  /**
   * Transform the data type by replacing VariantType with StringType.
   */
  private def transformDataType(dataType: DataType): DataType = {
    dataType match {
      case VariantType => StringType
      case st: StructType =>
        StructType(st.fields.map { field =>
          field.copy(dataType = transformDataType(field.dataType))
        })
      case other => other
    }
  }

  override protected def nullSafeEval(input: Any): Any = {
    transformValue(input, child.dataType)
  }

  private def transformValue(value: Any, dataType: DataType): Any = {
    if (value == null) {
      return null
    }

    dataType match {
      case VariantType =>
        val variantVal = value.asInstanceOf[VariantVal]
        val variant = new Variant(variantVal.getValue, variantVal.getMetadata)
        val z85String = DeltaStatsJsonUtils.encodeVariantAsZ85(variant)
        UTF8String.fromString(z85String)

      case st: StructType =>
        val row = value.asInstanceOf[InternalRow]
        val newValues = st.fields.zipWithIndex.map { case (field, i) =>
          val fieldValue = row.get(i, field.dataType)
          transformValue(fieldValue, field.dataType)
        }
        InternalRow.fromSeq(newValues)

      case _ =>
        value
    }
  }

  override def prettyName: String = "encode_variant_as_z85_string"

  override protected def withNewChildInternal(newChild: Expression)
      : EncodeNestedVariantAsZ85String = copy(child = newChild)
}
