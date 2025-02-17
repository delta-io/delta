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

import org.apache.spark.sql.types.AtomicType

/**
 * A type widening mode captures a specific set of type changes that are allowed to be applied.
 * Currently:
 *  - NoTypeWidening: No type change is allowed.
 *  - AllTypeWidening: All supported type widening changes are allowed.
 *  - TypeEvolution(uniformIcebergCompatibleOnly = true): Type changes that are eligible to be
 *    applied automatically during schema evolution and that are supported by Iceberg are allowed.
 *  - TypeEvolution(uniformIcebergCompatibleOnly = false): Type changes that are eligible to be
 *    applied automatically during schema evolution are allowed, even if they are not supported by
 *    Iceberg.
 *
 * AllTypeWidening & TypeEvolution also have a "bidirectional" variant: instead of only allowing
 * 'from' to be widened to 'to', these also allow 'to' to be widened to 'from'. Useful when there's
 * no actual relation between 'from' and 'to' and we just want to use the wider type of the two,
 * e.g. when merging two unrelated schemas.
 */
sealed trait TypeWideningMode {
  def getWidenedType(fromType: AtomicType, toType: AtomicType): Option[AtomicType]
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
   * All supported type widening changes are allowed. Unlike [[AllTypeWidening]], this also allows
   * widening `to` to `from`. Use for example when merging two unrelated schemas and we want just
   * want to get user the wider type of `from` and `to`.
   */
  case object AllTypeWideningBidirectional extends TypeWideningMode {
    override def getWidenedType(left: AtomicType, right: AtomicType): Option[AtomicType] =
      if (TypeWidening.isTypeChangeSupported(fromType = left, toType = right)) {
        Some(right)
      } else if (TypeWidening.isTypeChangeSupported(fromType = right, toType = left)) {
        Some(left)
      } else {
        None
      }
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
   * Type changes that are eligible to be applied automatically during schema evolution are allowed.
   * Can be restricted to only type changes supported by Iceberg. Unlike [[TypeEvolution]], this
   * also allows widening `to` to `from`. Use for example when merging two unrelated schemas and we
   * want just want to get user the wider type of `from` and `to`.
   */
  case class TypeEvolutionBidirectional(uniformIcebergCompatibleOnly: Boolean)
    extends TypeWideningMode {
    override def getWidenedType(fromType: AtomicType, toType: AtomicType): Option[AtomicType] =
      if (TypeWidening.isTypeChangeSupportedForSchemaEvolution(
        fromType = fromType, toType = toType, uniformIcebergCompatibleOnly)) {
        Some(toType)
      } else if (TypeWidening.isTypeChangeSupportedForSchemaEvolution(
        fromType = toType, toType = fromType, uniformIcebergCompatibleOnly)) {
        Some(fromType)
      } else {
        None
      }
  }
}
