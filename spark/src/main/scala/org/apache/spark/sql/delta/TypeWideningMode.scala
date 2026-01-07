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

import org.apache.spark.sql.catalyst.analysis.DecimalPrecisionTypeCoercion
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf.AllowAutomaticWideningMode
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql.types.{AtomicType, ByteType, DecimalType, IntegerType, IntegralType, LongType, ShortType}

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
sealed trait TypeWideningMode extends DeltaLogging {
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
   *
   * uniformIcebergCompatibleOnly: Restricts widenings to those supported by Iceberg.
   * allowAutomaticWidening: Controls widening behavior. Options:
   *   - 'always': enables all supported widenings,
   *   - 'same_family_type': uses default behavior,
   *   - 'never': disables all widenings.
   */
  case class TypeEvolution(
      uniformIcebergCompatibleOnly: Boolean,
      allowAutomaticWidening: AllowAutomaticWideningMode.Value) extends TypeWideningMode {
    override def getWidenedType(fromType: AtomicType, toType: AtomicType): Option[AtomicType] = {
      Option.when(canWiden(fromType, toType))(toType).orElse {
        logMissedWidening(fromType = fromType, toType = toType)
        None
      }
    }

    private def logMissedWidening(fromType: AtomicType, toType: AtomicType): Unit = {
      // Check if widening is possible under the least restricting conditions.
      val allowAllTypeEvolution = TypeEvolution(
        uniformIcebergCompatibleOnly = false,
        allowAutomaticWidening = AllowAutomaticWideningMode.ALWAYS)
      if (allowAllTypeEvolution.canWiden(fromType, toType)) {
        recordDeltaEvent(null,
          opType = "delta.typeWidening.missedAutomaticWidening",
          data = Map(
            "fromType" -> fromType.sql,
            "toType" -> toType.sql,
            "uniformIcebergCompatibleOnly" -> uniformIcebergCompatibleOnly,
            "allowAutomaticWidening" -> allowAutomaticWidening
          ))
      }
    }

    private def canWiden(fromType: AtomicType, toType: AtomicType): Boolean = {
      if (allowAutomaticWidening == AllowAutomaticWideningMode.ALWAYS) {
        TypeWidening.isTypeChangeSupported(
          fromType = fromType,
          toType = toType,
          uniformIcebergCompatibleOnly)
      } else if (allowAutomaticWidening == AllowAutomaticWideningMode.SAME_FAMILY_TYPE) {
        TypeWidening.isTypeChangeSupportedForSchemaEvolution(
          fromType = fromType, toType = toType, uniformIcebergCompatibleOnly)
      } else {
        false
      }
    }
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

  /**
   * Same as TypeEvolution with AllowAutomaticWideningMode.ALWAYS, but
   * additionally gets the wider decimal type given two types that are
   * DecimalType-compatible.
   */
  case object AllTypeWideningWithDecimalCoercion extends TypeWideningMode {
    private def getDecimalType(t: IntegralType): DecimalType = {
      t match {
        case _: ByteType => DecimalType(3, 0)
        case _: ShortType => DecimalType(5, 0)
        case _: IntegerType => DecimalType(10, 0)
        case _: LongType => DecimalType(20, 0)
      }
    }

    private def getWiderDecimalTypeWithInteger(
        integralType: IntegralType,
        decimalType: DecimalType): Option[DecimalType] = {
      val wider = DecimalPrecisionTypeCoercion.widerDecimalType(
        getDecimalType(integralType), decimalType)
      Option.when(
        TypeWidening.isTypeChangeSupported(getDecimalType(integralType), wider) &&
          TypeWidening.isTypeChangeSupported(decimalType, wider))(wider)
    }

    override def getWidenedType(fromType: AtomicType, toType: AtomicType): Option[AtomicType] =
      (fromType, toType) match {
        case (from, to) if TypeWidening.isTypeChangeSupported(from, to) => Some(to)
        case (l: IntegralType, r: DecimalType) =>
          getWiderDecimalTypeWithInteger(l, r)
        case (l: DecimalType, r: IntegralType) =>
          getWiderDecimalTypeWithInteger(r, l)
        case (l: DecimalType, r: DecimalType) =>
          val wider = DecimalPrecisionTypeCoercion.widerDecimalType(l, r)
          Option.when(
            TypeWidening.isTypeChangeSupported(l, wider) &&
              TypeWidening.isTypeChangeSupported(r, wider))(wider)
        case _ => None
      }
  }

  /**
   * Same as TypeEvolution with AllowAutomaticWideningMode.SAME_FAMILY_TYPE,
   * but additionally gets the wider decimal type given two types that are
   * DecimalType-compatible.
   */
  case object TypeEvolutionWithDecimalCoercion extends TypeWideningMode {
    override def getWidenedType(fromType: AtomicType, toType: AtomicType): Option[AtomicType] = {
      def typeChangeSupported: (AtomicType, AtomicType) => Boolean =
        TypeWidening.isTypeChangeSupportedForSchemaEvolution(_, _,
          uniformIcebergCompatibleOnly = false)

      (fromType, toType) match {
        case (from, to) if typeChangeSupported(from, to) => Some(to)
        case (l: DecimalType, r: DecimalType) =>
          val wider = DecimalPrecisionTypeCoercion.widerDecimalType(l, r)
          Option.when(typeChangeSupported(l, wider) && typeChangeSupported(r, wider))(wider)
        case _ => None
      }
    }
  }
}
