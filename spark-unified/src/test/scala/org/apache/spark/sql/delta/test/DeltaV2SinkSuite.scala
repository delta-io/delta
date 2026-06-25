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

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.DeltaSinkSuite

/**
 * Test suite that runs DeltaSinkSuite using the V2 connector (V2_ENABLE_MODE=STRICT).
 */
class DeltaV2SinkSuite extends DeltaSinkSuite with V2ForceTest {

  override protected def shouldPassTests: Set[String] = DeltaV2SinkSuite.PassingTests

  override protected def shouldFailTests: Set[String] = DeltaV2SinkSuite.FailingTests
}

/**
 * Shared V2-connector test classifications for `DeltaSinkSuite`.
 */
object DeltaV2SinkSuite {

  // Only tests migrated to name-based access (write via .toTable, read via spark.read.table) are
  // listed here -- these are the ones that actually route through the catalog (and thus the V2
  // Kernel sink) under STRICT and pass. Path-based tests are intentionally NOT here: under STRICT a
  // path-based write still goes through the V1 sink (it bypasses the catalog), so listing it as a
  // V2 "pass" would be misleading and would merely duplicate DeltaSinkSuite's V1 run -- those are
  // moved to FailingTests (ignored) instead.
  val PassingTests: Set[String] = Set(
    "append mode",
    "work with aggregation + watermark",
    "do not trust user nullability, so that parquet files aren't corrupted",
    "can't write out with all columns being partition columns"
  )

  val FailingTests: Set[String] = Set(
    // ---- Path-based / V1-only tests ----
    // under STRICT these bypass the catalog and run on the V1 sink, so they don't exercise V2 and
    // would just duplicate DeltaSinkSuite.
    "path not specified",
    "DeltaSink.catalogTable is correctly populated - path-based table",
    "incompatible schema merging throws errors - first streaming then batch",
    "DeltaSink.deltaLog is not initialized in DeltaSink constructor",

    // ---- Genuine V2-sink gaps (name-based tests that route to V2 and fail there):
    // V2-sink gap: the Kernel sink writes all data to the table root with empty partition values,
    // so a partitioned target fails with "Partition values provided are not matching the partition
    // columns. Partition columns: [id], Partition values: {}".
    "partitioned writing and batch reading",
    // Same V2-sink gap (no partition support): this test partitions by `value` to exercise
    // partition-value path encoding. Migratable to name-based (passes on V1).
    "SPARK-21167: encode and decode path correctly",
    // Same V2-sink gap (no partition support): the partitioned streaming write fails before the
    // batch-partitioning-mismatch assertion is reached. Migratable to name-based (passes on V1).
    "throw exception when users are trying to write in batch with different partitioning",
    // V2-sink gap: the basic Kernel sink dropped the V1 NullType guard, so writing a UDT containing
    // NullType is not rejected (no exception). Migratable to name-based (passes on V1, which keeps
    // the DeltaSink guard).
    "DeltaSink rejects DataFrame with UDT containing NullType",
    // V2-sink gap: the Kernel commit does not set isBlindAppend=true in the CommitInfo
    // (isLastCommitBlindAppend was false for a simple append).
    "streaming write correctly sets isBlindAppend in CommitInfo",
    // V2-sink gap: DeltaV2StreamingWrite ignores the userMetadata option, so it is absent from
    // history (None vs Some("testMeta!")).
    "history includes user-defined metadata for DataFrame.writeStream API",
    // Update IS rejected on V2 -- DeltaV2WriteBuilder deliberately does not implement
    // SupportsStreamingUpdateAsAppend, so StreamExecution.createWrite fails the query with
    // "<table> does not support Update mode." But that require() fires asynchronously on the query
    // thread as an IllegalArgumentException, whereas this test expects a synchronous
    // AnalysisException at start time (the V1 createSink behavior). Migratable to name-based (V1).
    "update mode: not supported",
    // Complete IS rejected on V2 -- DeltaV2WriteBuilder deliberately does not implement
    // SupportsTruncate, so createWrite fails with "<table> does not support Complete mode" (Kernel
    // also has no REPLACE_TABLE / RemoveFile API). The test expects Complete to succeed, so it fails
    // here. Migratable to name-based (passes on V1).
    "complete mode",
    // Under V2 the catalog-based streaming write produces a WriteToMicroBatchDataSource over a
    // DeltaV2Table (DeltaV2StreamingWrite), but this test pattern-matches the V1
    // WriteToMicroBatchDataSourceV1/DeltaSink plan to inspect DeltaSink.catalogTable -- a V1-only
    // concept. Throws scala.MatchError in verifyDeltaSinkCatalog under the V2 connector.
    "DeltaSink.catalogTable is correctly populated - catalog-based table"
  )
}
