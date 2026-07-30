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
import org.scalatest.time.SpanSugar._

/**
 * Test suite that runs DeltaSinkSuite using the V2 connector (V2_ENABLE_MODE=STRICT).
 */
class DeltaV2SinkSuite extends DeltaSinkSuite with V2ForceTest {

  // Route helper-based writes/reads through the catalog by name (.toTable / spark.read.table) so
  // that, combined with V2_ENABLE_MODE=STRICT (from V2ForceTest), they exercise the V2 Kernel sink.
  // Without this the base suite defaults to path-based access, which bypasses the catalog and runs
  // on the V1 sink even under STRICT.
  override protected def useDsv2: Boolean = true

  // `toTable` runs a one-time CreateTable before the query starts (~10s); per-batch writes are
  // unchanged. That fixed overhead makes the 60s default too tight under CI contention. Matches
  // DeltaSinkNameBasedSuite, which sets the same timeout for the same reason.
  override val streamingTimeout = 90.seconds

  override protected def shouldPassTests: Set[String] = DeltaV2SinkSuite.PassingTests

  override protected def shouldFailTests: Set[String] = DeltaV2SinkSuite.FailingTests
}

/**
 * Shared V2-connector test classifications for `DeltaSinkSuite`.
 */
object DeltaV2SinkSuite {

  // Tests migrated to name-based access (.toTable / spark.read.table) that route through the
  // catalog to the V2 Kernel sink under STRICT and pass. Path-based tests are NOT here: under
  // STRICT they still hit the V1 sink (bypassing the catalog), so they go in FailingTests (ignored)
  // rather than duplicate DeltaSinkSuite's V1 run.
  val PassingTests: Set[String] = Set(
    "append mode",
    "work with aggregation + watermark",
    "do not trust user nullability, so that parquet files aren't corrupted",
    "can't write out with all columns being partition columns"
  )

  val FailingTests: Set[String] = Set(
    // ---- Path-based / V1-only: bypass the catalog under STRICT, so they'd duplicate the V1 run.
    "path not specified",
    "DeltaSink.catalogTable is correctly populated - path-based table",
    "incompatible schema merging throws errors - first streaming then batch",
    "DeltaSink.deltaLog is not initialized in DeltaSink constructor",
    // Path-based schema evolution: under STRICT these still hit the V1 sink (no catalog), so they
    // would duplicate the base suite's V1 run rather than exercise the V2 Kernel sink.
    "allow schema evolution after dropping column",
    "allow schema evolution after renaming column",

    // ---- Genuine V2-sink gaps (name-based, route to V2, fail there) ----
    // No partition support: the V2 write rejects a partitioned target, surfaced as an async
    // StreamingQueryException, not the outcome these tests expect.
    "partitioned writing and batch reading",
    "SPARK-21167: encode and decode path correctly",
    "throw exception when users are trying to write in batch with different partitioning",
    // No NullType support, creating a table with void column fails.
    "DeltaSink rejects streaming write to table with generated void column",
    "DeltaSink allows streaming write to table with non-generated void column",
    "DeltaSink rejects streaming write with NullType column in batch schema",
    // Kernel sink dropped the V1 NullType guard, so a UDT containing NullType is not rejected.
    "DeltaSink rejects DataFrame with UDT containing NullType",
    // Kernel commit does not set isBlindAppend=true in CommitInfo.
    "streaming write correctly sets isBlindAppend in CommitInfo",
    // userMetadata is rejected (it's in UNSUPPORTED_STREAMING_OPTIONS), so the query throws at
    // start; the test expects the metadata recorded in history, not a failure.
    "history includes user-defined metadata for DataFrame.writeStream API",
    // Update/Complete ARE rejected on V2 (builder has neither SupportsStreamingUpdateAsAppend nor
    // SupportsTruncate), but as an async IllegalArgumentException on the query thread, not the sync
    // start-time AnalysisException these tests expect.
    "update mode: not supported",
    "complete mode",
    // Pattern-matches the V1 WriteToMicroBatchDataSourceV1/DeltaSink plan; MatchError under V2.
    "DeltaSink.catalogTable is correctly populated - catalog-based table"
  )
}
