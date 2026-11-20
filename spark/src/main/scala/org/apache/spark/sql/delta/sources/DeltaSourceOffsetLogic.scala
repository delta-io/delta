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

package org.apache.spark.sql.delta.sources

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaErrors, TypeWidening}
import org.apache.spark.sql.delta.actions.Metadata

import org.apache.spark.sql.SparkSession

/**
 * Offset logic shared by the Delta streaming sources: [[DeltaSource]] (V1) and
 * `DeltaV2MicroBatchStream` (V2). Both sources mix in this trait and run the same code.
 */
trait DeltaSourceOffsetLogic {

  // ---------------------------------------------------------------------------------------------
  // Required from the source
  // ---------------------------------------------------------------------------------------------

  protected def spark: SparkSession

  /** Options that control how the source handles schema changes. */
  protected def schemaReadOptions: DeltaStreamUtils.SchemaReadOptions

  /** Whether the source reads with a schema from the metadata tracking log. */
  protected def trackingMetadataChange: Boolean


  // ---------------------------------------------------------------------------------------------
  // Schema checks
  // ---------------------------------------------------------------------------------------------

  /**
   * Checks for non-additive schema changes (column renames, drops, type widening) and blocks
   * the stream by throwing an exception if detected.
   *
   * Blocks when type widening tracking is enabled and widening changes exist, or when column
   * mapping changes (rename/drop) are detected, unless `allowUnsafeStreamingReadOnColumnMapping
   * SchemaChanges` is enabled. Upon blocking, the error requests the user to provide a schema
   * tracking location to enable schema tracking. On restart, users must acknowledge changes via
   * reader options or SQL confs.
   * See [[DeltaSourceMetadataEvolutionSupport.validateIfSchemaChangeCanBeUnblocked]].
   *
   * Note: Should not be called when schema tracking is active (trackingMetadataChange = true).
   *
   * @param oldMetadata Previous metadata (typically from stream initialization)
   * @param newMetadata New metadata with potential schema changes
   * @param validatedDuringStreamStart Whether validating during stream start vs. execution,
   *                                   which affects the error message.
   * @throws DeltaAnalysisException if non-additive schema changes require blocking
   */
  protected def checkNonAdditiveSchemaChanges(
      oldMetadata: Metadata,
      newMetadata: Metadata,
      validatedDuringStreamStart: Boolean): Unit = {
    val shouldTrackSchema: Boolean =
      if (schemaReadOptions.typeWideningEnabled &&
          schemaReadOptions.enableSchemaTrackingForTypeWidening &&
        TypeWidening.containsWideningTypeChanges(oldMetadata.schema, newMetadata.schema)) {
        // If schema tracking is enabled for type widening, we will detect widening type changes and
        // block the stream until the user sets `allowSourceColumnTypeChange` - similar to handling
        // DROP/RENAME for column mapping.
        true
      } else if (schemaReadOptions.allowUnsafeStreamingReadOnColumnMappingSchemaChanges) {
        false
      } else {
        // Column mapping schema changes
        assert(!trackingMetadataChange, "should not check schema change while tracking it")
        !DeltaColumnMapping.hasNoColumnMappingSchemaChanges(newMetadata, oldMetadata,
          schemaReadOptions.allowUnsafeStreamingReadOnPartitionColumnChanges)
      }

    if (shouldTrackSchema) {
      throw DeltaErrors.blockStreamingReadsWithIncompatibleNonAdditiveSchemaChanges(
        spark,
        oldMetadata.schema,
        newMetadata.schema,
        detectedDuringStreaming = !validatedDuringStreamStart)
    }
  }
}
