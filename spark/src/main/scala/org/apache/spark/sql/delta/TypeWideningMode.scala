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

import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql.catalyst.analysis.DecimalPrecisionTypeCoercion
import org.apache.spark.sql.types.{AtomicType, DecimalType}

/**
 * A type widening mode captures a specific set of type changes that are allowed to be applied.
 * Currently:
 *  - NoTypeWidening: No type change is allowed.
 *  - AllTypeWidening: Allows widening to the target type using any supported type change.
 *  - TypeEvolution: Only allows widening to the target type if the type change is eligible to be
 *      applied automatically during schema evolution.
 *  - AllTypeWideningToCommonWiderType: Allows widening to a common (possibly different) wider type
 *      using any supported type change.
 *  - TypeEvolutionToCommonWiderType: Allows widening to a common (possibly different) wider type
 *      using only type changes that are eligible to be applied automatically during schema
 *      evolution.
 *
 * TypeEvolution modes can be restricted to only type changes supported by Iceberg by passing
 * `uniformIcebergCompatibleOnly = truet`, to ensure that we don't automatically apply a type change
 * that would break Iceberg compatibility.
 */
sealed trait TypeWideningMode {
  def getWidenedType(fromType: AtomicType, toType: AtomicType): Option[AtomicType]

  def shouldWidenTo(fromType: AtomicType, toType: AtomicType): Boolean =
    getWidenedType(fromType, toType).contains(toType)
}

object TypeWideningMode {
  /**
   * No type change allowed. Typically because type widening and/or schema evolution isn't enabled.
   */
  case object NoTypeWidening extends TypeWideningMode {
    override def getWidenedType(fromType: AtomicType, toType: AtomicType): Option[AtomicType] = None
  }

  /** All supported type widening changes are allowed. */
  case object AllTypeWidening extends TypeWideningMode {
    override def getWidenedType(fromType: AtomicType, toType: AtomicType): Option[AtomicType] =
      Option.when(TypeWidening.isTypeChangeSupported(fromType = fromType, toType = toType))(toType)
  }

  /**
   * Type changes that are eligible to be applied automatically during schema evolution are allowed.
   * Can be restricted to only type changes supported by Iceberg.
   */
  case class TypeEvolution(uniformIcebergCompatibleOnly: Boolean) extends TypeWideningMode {
    override def getWidenedType(fromType: AtomicType, toType: AtomicType): Option[AtomicType] =
        Option.when(TypeWidening.isTypeChangeSupportedForSchemaEvolution(
          fromType = fromType, toType = toType, uniformIcebergCompatibleOnly))(toType)
  }

  /**
   * All supported type widening changes are allowed. Unlike [[AllTypeWidening]], this also allows
   * widening `to` to `from`, and for decimals, widening to a different decimal type that is wider
   * than both input types. Use for example when merging two unrelated schemas and we want just want
   * to find a wider schema to use.
   */
  case object AllTypeWideningToCommonWiderType extends TypeWideningMode {
    override def getWidenedType(left: AtomicType, right: AtomicType): Option[AtomicType] =
      (left, right) match {
        case (l, r) if TypeWidening.isTypeChangeSupported(l, r) => Some(r)
        case (l, r) if TypeWidening.isTypeChangeSupported(r, l) => Some(l)
        case (l: DecimalType, r: DecimalType) =>
          val wider = DecimalPrecisionTypeCoercion.widerDecimalType(l, r)
          Option.when(
            TypeWidening.isTypeChangeSupported(l, wider) &&
            TypeWidening.isTypeChangeSupported(r, wider))(wider)
        case _ => None
      }
  }

  /**
   * Type changes that are eligible to be applied automatically during schema evolution are allowed.
   * Can be restricted to only type changes supported by Iceberg. Unlike [[TypeEvolution]], this
   * also allows widening `to` to `from`, and for decimals, widening to a different decimal type
   * that is wider han both input types. Use for example when merging two unrelated schemas and we
   * want just want to find a wider schema to use.
   */
  case class TypeEvolutionToCommonWiderType(uniformIcebergCompatibleOnly: Boolean)
    extends TypeWideningMode {
    override def getWidenedType(left: AtomicType, right: AtomicType): Option[AtomicType] = {
      def typeChangeSupported: (AtomicType, AtomicType) => Boolean =
        TypeWidening.isTypeChangeSupportedForSchemaEvolution(_, _, uniformIcebergCompatibleOnly)

      (left, right) match {
        case (l, r) if typeChangeSupported(l, r) => Some(r)
        case (l, r) if typeChangeSupported(r, l) => Some(l)
        case (l: DecimalType, r: DecimalType) =>
          val wider = DecimalPrecisionTypeCoercion.widerDecimalType(l, r)
          Option.when(typeChangeSupported(l, wider) && typeChangeSupported(r, wider))(wider)
        case _ => None
      }
    }
  }
}
