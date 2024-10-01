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

import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol, TableFeatureProtocolUtils}

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

object TypeWidening {

  /**
   * Returns whether the protocol version supports the Type Widening table feature.
   */
  def isSupported(protocol: Protocol): Boolean =
    Seq(TypeWideningPreviewTableFeature, TypeWideningTableFeature)
      .exists(protocol.isFeatureSupported)

  /**
   * Returns whether Type Widening is enabled on this table version. Checks that Type Widening is
   * supported, which is a pre-requisite for enabling Type Widening, throws an error if
   * not. When Type Widening is enabled, the type of existing columns or fields can be widened
   * using ALTER TABLE CHANGE COLUMN.
   */
  def isEnabled(protocol: Protocol, metadata: Metadata): Boolean = {
    val isEnabled = DeltaConfigs.ENABLE_TYPE_WIDENING.fromMetaData(metadata)
    if (isEnabled && !isSupported(protocol)) {
      throw new IllegalStateException(
        s"Table property '${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' is " +
          s"set on the table but this table version doesn't support table feature " +
          s"'${TableFeatureProtocolUtils.propertyKey(TypeWideningTableFeature)}'.")
    }
    isEnabled
  }

  /**
   * Checks that the type widening table property wasn't disabled or enabled between the two given
   * states, throws an errors if it was.
   */
  def ensureFeatureConsistentlyEnabled(
      protocol: Protocol,
      metadata: Metadata,
      otherProtocol: Protocol,
      otherMetadata: Metadata): Unit = {
    if (isEnabled(protocol, metadata) != isEnabled(otherProtocol, otherMetadata)) {
      throw DeltaErrors.metadataChangedException(None)
    }
  }

  /**
   * Returns whether the given type change is eligible for widening. This only checks atomic types.
   * It is the responsibility of the caller to recurse into structs, maps and arrays.
   */
  def isTypeChangeSupported(fromType: AtomicType, toType: AtomicType): Boolean =
    TypeWideningShims.isTypeChangeSupported(fromType, toType)

  /**
   * Returns whether the given type change can be applied during schema evolution. Only a
   * subset of supported type changes are considered for schema evolution.
   */
  def isTypeChangeSupportedForSchemaEvolution(fromType: AtomicType, toType: AtomicType): Boolean =
    TypeWideningShims.isTypeChangeSupportedForSchemaEvolution(fromType, toType)

  /**
   * Asserts that the given table doesn't contain any unsupported type changes. This should never
   * happen unless a non-compliant writer applied a type change that is not part of the feature
   * specification.
   */
  def assertTableReadable(protocol: Protocol, metadata: Metadata): Unit = {
    if (!isSupported(protocol) ||
      !TypeWideningMetadata.containsTypeWideningMetadata(metadata.schema)) {
      return
    }

    TypeWideningMetadata.getAllTypeChanges(metadata.schema).foreach {
      case (_, TypeChange(_, from: AtomicType, to: AtomicType, _))
        if isTypeChangeSupported(from, to) =>
      case (fieldPath, TypeChange(_, from: AtomicType, to: AtomicType, _))
        if stableFeatureCanReadTypeChange(from, to) =>
        val featureName = if (protocol.isFeatureSupported(TypeWideningPreviewTableFeature)) {
          TypeWideningPreviewTableFeature
        } else {
          TypeWideningTableFeature
        }
        throw DeltaErrors.unsupportedTypeChangeInPreview(fieldPath, from, to, featureName)
      case (fieldPath, invalidChange) =>
        throw DeltaErrors.unsupportedTypeChangeInSchema(
          fieldPath ++ invalidChange.fieldPath,
          invalidChange.fromType,
          invalidChange.toType
        )
    }
  }

  /**
   * Whether the given type change is supported in the stable version of the feature. Used to
   * provide a helpful error message during the preview phase if upgrading to Delta 4.0 would allow
   * reading the table.
   */
  private def stableFeatureCanReadTypeChange(fromType: AtomicType, toType: AtomicType): Boolean =
    (fromType, toType) match {
      case (from, to) if from == to => true
      case (from: IntegralType, to: IntegralType) => from.defaultSize <= to.defaultSize
      case (FloatType, DoubleType) => true
      case (DateType, TimestampNTZType) => true
      case (ByteType | ShortType | IntegerType, DoubleType) => true
      case (from: DecimalType, to: DecimalType) => to.isWiderThan(from)
      // Byte, Short, Integer are all stored as INT32 in parquet. The parquet readers support
      // converting INT32 to Decimal(10, 0) and wider.
      case (ByteType | ShortType | IntegerType, d: DecimalType) => d.isWiderThan(IntegerType)
      // The parquet readers support converting INT64 to Decimal(20, 0) and wider.
      case (LongType, d: DecimalType) => d.isWiderThan(LongType)
      case _ => false
    }
}
