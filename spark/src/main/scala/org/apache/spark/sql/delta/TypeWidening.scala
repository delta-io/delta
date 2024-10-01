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

import org.apache.spark.sql.delta.actions.{Metadata, Protocol, TableFeatureProtocolUtils}

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
        if TypeWideningShims.canReadTypeChange(from, to) =>
      case (fieldPath, invalidChange) =>
        throw DeltaErrors.unsupportedTypeChangeInSchema(
          fieldPath ++ invalidChange.fieldPath,
          invalidChange.fromType,
          invalidChange.toType
        )
    }
  }
}
