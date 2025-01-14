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
 */
sealed trait TypeWideningMode {
  def shouldWidenType(fromType: AtomicType, toType: AtomicType): Boolean
}

object TypeWideningMode {
  /**
   * No type change allowed. Typically because type widening and/or schema evolution isn't enabled.
   */
  case object NoTypeWidening extends TypeWideningMode {
    override def shouldWidenType(fromType: AtomicType, toType: AtomicType): Boolean = false
  }

  /** All supported type widening changes are allowed. */
  case object AllTypeWidening extends TypeWideningMode {
    override def shouldWidenType(fromType: AtomicType, toType: AtomicType): Boolean =
      TypeWidening.isTypeChangeSupported(fromType = fromType, toType = toType)
  }

  /**
   * Type changes that are eligible to be applied automatically during schema evolution are allowed.
   * Can be restricted to only type changes supported by Iceberg.
   */
  case class TypeEvolution(uniformIcebergCompatibleOnly: Boolean) extends TypeWideningMode {
    override def shouldWidenType(fromType: AtomicType, toType: AtomicType): Boolean =
        TypeWidening.isTypeChangeSupportedForSchemaEvolution(
          fromType = fromType, toType = toType, uniformIcebergCompatibleOnly)
  }
}
