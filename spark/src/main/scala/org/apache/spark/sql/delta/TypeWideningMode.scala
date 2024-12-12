package org.apache.spark.sql.delta

import org.apache.spark.sql.types.AtomicType

/**
 * A type widening mode captures a specific set of type changes that are allowed to be applied.
 * Currently:
 *  - NoTypeWidening: No type change is allowed.
 *  - TypeEvolution(uniformIcebergEnabled = true): Type changes that are eligible to be applied
 *    automatically during schema evolution and that are supported by Iceberg are allowed.
 *  - TypeEvolution(uniformIcebergEnabled = false): Type changes that are eligible to be applied
 *    automatically during schema evolution are allowed, even if they are not supported by Iceberg.
 */
trait TypeWideningMode {
  def shouldWidenType(fromType: AtomicType, toType: AtomicType): Boolean
}

object TypeWideningMode {
  /**
   * No type change allowed. Typically because type widening and/or schema evolution isn't enabled.
   */
  object NoTypeWidening extends TypeWideningMode {
    override def shouldWidenType(fromType: AtomicType, toType: AtomicType): Boolean = false
  }

  /**
   * Type changes that are eligible to be applied automatically during schema evolution are allowed.
   * Can be restricted to only type changes supported by Iceberg.
   */
  case class TypeEvolution(uniformIcebergEnabled: Boolean) extends TypeWideningMode {
    override def shouldWidenType(fromType: AtomicType, toType: AtomicType): Boolean =
        TypeWidening.isTypeChangeSupportedForSchemaEvolution(
          fromType = fromType, toType = toType, uniformIcebergEnabled)
  }
}
