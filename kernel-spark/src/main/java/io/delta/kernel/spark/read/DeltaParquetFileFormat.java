/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.spark.read;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import org.apache.spark.sql.delta.DeltaParquetFileFormatBase;
import scala.Option;

/**
 * Kernel-based implementation of Delta's custom Parquet file format.
 *
 * <p>This class extends {@link DeltaParquetFileFormatBase} and uses {@link KernelDeltaWrapper} to
 * bridge between Kernel's {@link Protocol} and {@link Metadata} and Spark's Delta file format
 * requirements.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li>Column Mapping: Converts logical schema to physical schema using Kernel APIs
 *   <li>Deletion Vectors: Filters out deleted rows during read
 *   <li>Row Tracking: Exposes row_id and row_commit_version metadata columns
 * </ul>
 */
public class DeltaParquetFileFormat extends DeltaParquetFileFormatBase {

  private final Protocol protocol;
  private final Metadata metadata;
  private final boolean nullableRowTrackingConstantFields;
  private final boolean nullableRowTrackingGeneratedFields;
  private final boolean optimizationsEnabled;
  private final scala.Option<String> tablePath;
  private final boolean isCDCRead;

  /**
   * Creates a new DeltaParquetFileFormat with default settings.
   *
   * @param protocol Kernel's Protocol
   * @param metadata Kernel's Metadata
   */
  public DeltaParquetFileFormat(Protocol protocol, Metadata metadata) {
    this(
        protocol,
        metadata,
        false, // nullableRowTrackingConstantFields
        false, // nullableRowTrackingGeneratedFields
        true, // optimizationsEnabled
        Option.empty(), // tablePath
        false); // isCDCRead
  }

  /**
   * Creates a new DeltaParquetFileFormat with all configurable parameters.
   *
   * @param protocol Kernel's Protocol
   * @param metadata Kernel's Metadata
   * @param nullableRowTrackingConstantFields whether constant row tracking fields are nullable
   * @param nullableRowTrackingGeneratedFields whether generated row tracking fields are nullable
   * @param optimizationsEnabled whether optimizations are enabled
   * @param tablePath optional table path for DV resolution
   * @param isCDCRead whether this is a CDC read
   */
  public DeltaParquetFileFormat(
      Protocol protocol,
      Metadata metadata,
      boolean nullableRowTrackingConstantFields,
      boolean nullableRowTrackingGeneratedFields,
      boolean optimizationsEnabled,
      scala.Option<String> tablePath,
      boolean isCDCRead) {
    super(
        new KernelDeltaWrapper(protocol, metadata),
        nullableRowTrackingConstantFields,
        nullableRowTrackingGeneratedFields,
        optimizationsEnabled,
        tablePath,
        isCDCRead);
    this.protocol = protocol;
    this.metadata = metadata;
    this.nullableRowTrackingConstantFields = nullableRowTrackingConstantFields;
    this.nullableRowTrackingGeneratedFields = nullableRowTrackingGeneratedFields;
    this.optimizationsEnabled = optimizationsEnabled;
    this.tablePath = tablePath;
    this.isCDCRead = isCDCRead;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    DeltaParquetFileFormat that = (DeltaParquetFileFormat) other;
    return java.util.Objects.equals(protocol, that.protocol)
        && java.util.Objects.equals(metadata, that.metadata)
        && nullableRowTrackingConstantFields == that.nullableRowTrackingConstantFields
        && nullableRowTrackingGeneratedFields == that.nullableRowTrackingGeneratedFields
        && optimizationsEnabled == that.optimizationsEnabled
        && java.util.Objects.equals(tablePath, that.tablePath)
        && isCDCRead == that.isCDCRead;
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(
        protocol,
        metadata,
        nullableRowTrackingConstantFields,
        nullableRowTrackingGeneratedFields,
        optimizationsEnabled,
        tablePath,
        isCDCRead);
  }

  @Override
  public String toString() {
    return String.format(
        "DeltaParquetFileFormat(protocol=%s, metadata=%s, "
            + "nullableRowTrackingConstantFields=%s, "
            + "nullableRowTrackingGeneratedFields=%s, "
            + "optimizationsEnabled=%s, tablePath=%s, isCDCRead=%s)",
        protocol,
        metadata,
        nullableRowTrackingConstantFields,
        nullableRowTrackingGeneratedFields,
        optimizationsEnabled,
        tablePath,
        isCDCRead);
  }
}
