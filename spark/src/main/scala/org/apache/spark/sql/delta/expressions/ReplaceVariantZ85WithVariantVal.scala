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
import org.apache.spark.types.variant.{Variant, VariantUtil}
import org.apache.spark.unsafe.types.VariantVal

/**
 * An expression that replaces Z85-encoded variant strings with decoded VariantVals.
 *
 * When parsing JSON stats with variant fields, the variants are initially encoded as Z85 strings.
 * The standard from_json treats these as regular strings and creates VariantVal objects that
 * contain the Z85 string representation. This expression walks through the result and decodes
 * any Z85-encoded variants to their proper binary representation.
 *
 * @param child The expression producing the row with Z85-encoded variants
 */
case class ReplaceVariantZ85WithVariantVal(child: Expression)
  extends UnaryExpression with CodegenFallback {

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!isValidType(child.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        s"ReplaceVariantZ85WithVariantVal does not support arrays or maps in schema. " +
        s"Found: ${child.dataType}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  private def isValidType(dataType: DataType): Boolean = {
    dataType match {
      case _: ArrayType => false
      case _: MapType => false
      case st: StructType =>
        st.fields.forall(field => isValidType(field.dataType))
      case _ => true
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
        if (VariantUtil.getType(variant.getValue, 0) == VariantUtil.Type.STRING) {
          val z85String = variant.getString()
          DeltaStatsJsonUtils.decodeVariantFromZ85(z85String)
        } else {
          throw new IllegalStateException(
            s"Expected Z85-encoded variant string but got type " +
              s"${VariantUtil.getType(variant.getValue, 0)}")
        }

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

  override def prettyName: String = "replace_variant_z85_with_variant_val"

  override protected def withNewChildInternal(newChild: Expression)
    : ReplaceVariantZ85WithVariantVal = copy(child = newChild)
}
