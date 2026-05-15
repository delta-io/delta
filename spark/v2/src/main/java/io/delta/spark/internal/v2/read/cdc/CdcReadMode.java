/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.spark.internal.v2.read.cdc;

/**
 * Distinguishes how a CDC read enters the partition-reader factory.
 *
 * <p>Two pathways exist in the V2 connector:
 *
 * <ul>
 *   <li>{@link #STREAMING}: streaming CDC reads ({@code spark.readStream...option("readChangeFeed",
 *       "true")}) route through {@code SparkMicroBatchStream}. PartitionUtils owns CDC schema
 *       augmentation and tail-column injection via {@code CDCReadFunction}.
 *   <li>{@link #BATCH_CHANGELOG}: Auto-CDF batch reads ({@code SELECT * FROM t CHANGES FROM ...})
 *       route through {@code TableCatalog.loadChangelog} into {@code DeltaChangelogBatch}, which
 *       wraps the partition reader with its own {@code CDCPartitionReaderFactory} that injects the
 *       CDC tail columns as per-partition constants. PartitionUtils therefore leaves the schema and
 *       reader untouched on the CDC side to avoid double injection.
 * </ul>
 *
 * <p>Both pathways are CDC-aware at the parquet-format level (see {@code
 * DeltaParquetFileFormatV2}); {@link #isCdc()} reflects that. Only the schema-augmentation and
 * inner reader-wrap hooks are gated by {@link #injectsCdcAtReaderLevel()}.
 */
public enum CdcReadMode {
  /** Non-CDC scan. */
  NONE,
  /** Streaming CDC read; PartitionUtils owns CDC schema + tail-column injection. */
  STREAMING,
  /** Auto-CDF batch read; outer {@code CDCPartitionReaderFactory} owns CDC tail injection. */
  BATCH_CHANGELOG;

  /** True iff this is any flavor of CDC read (affects the underlying parquet file format). */
  public boolean isCdc() {
    return this != NONE;
  }

  /**
   * True iff PartitionUtils should augment the read schema with CDC tail columns and wrap the
   * reader with {@code CDCReadFunction}. False when an outer wrapper handles CDC injection.
   */
  public boolean injectsCdcAtReaderLevel() {
    return this == STREAMING;
  }
}
