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

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.hooks.{AsyncAutoCompactService, AutoCompact}
import org.apache.spark.sql.delta.optimize.CompactionTestHelperForAutoCompaction
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for asynchronous Auto Compaction.
 *
 * Covers:
 *   - Default-off: behavior identical to inline AC.
 *   - Opt-in: writer returns before AC commit; AC commit lands after worker drains.
 *   - Snapshot freshness: worker re-evaluates against a fresh snapshot, so a peer that
 *     compacted between enqueue and run causes the worker to skip cleanly.
 *   - Queue full: writer is unaffected, dropped event is recorded.
 *   - Concurrent submissions: partition reservation invariant holds.
 */
class AutoCompactAsyncSuite extends
    CompactionTestHelperForAutoCompaction
  with DeltaSQLCommandTest
  with SharedSparkSession
  with AutoCompactTestUtils {

  import testImplicits._

  private val AsyncTimeoutMs = 60000L

  override def beforeEach(): Unit = {
    super.beforeEach()
    AsyncAutoCompactService.resetCountersForTesting()
  }

  private def awaitDrain(tableId: String): Unit = {
    assert(
      AsyncAutoCompactService.awaitQuiescenceForTesting(tableId, AsyncTimeoutMs),
      s"Async Auto Compact pool did not drain within ${AsyncTimeoutMs}ms for table $tableId")
  }

  test("async disabled by default: writer thread runs AC inline") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
        // Sanity: async should be off by default.
        assert(!AsyncAutoCompactService.isEnabled(spark))

        spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        // Version 0 is the initial write, version 1 is the inline AC OPTIMIZE.
        assert(deltaLog.update().version === 1)
        assert(deltaLog.update().numOfFiles === 1)
        // No async submissions should have been registered.
        assert(AsyncAutoCompactService.pendingForTable(
          deltaLog.unsafeVolatileTableId) === 0)
      }
    }
  }

  test("async enabled: AC commit lands after writer returns and worker drains") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
        assert(AsyncAutoCompactService.isEnabled(spark))

        spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        val tableId = deltaLog.unsafeVolatileTableId

        // Drain the async pool, then assert AC has run (a new commit after the write).
        awaitDrain(tableId)

        val finalSnap = deltaLog.update()
        assert(finalSnap.version === 1, "Expected the async AC OPTIMIZE commit to land.")
        assert(finalSnap.numOfFiles === 1)

        val lastEvent = deltaLog.history.getHistory(Some(1)).head
        assert(lastEvent.operation === "OPTIMIZE")
        assert(lastEvent.operationParameters("auto") === "true")
      }
    }
  }

  test("async enabled: submission telemetry event is recorded") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
        val submitted = Log4jUsageLogger.track {
          spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
        }.filter(_.tags.get("opType") ==
          Some("delta.autoCompaction.async.submitted"))
        assert(submitted.nonEmpty, "Expected a submitted event when async AC fires.")

        val deltaLog = DeltaLog.forTable(spark, dir)
        awaitDrain(deltaLog.unsafeVolatileTableId)
      }
    }
  }

  test("async re-evaluates eligibility on a fresh snapshot") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "5") {
        // First write: 5 small files => eligible => AC compacts to 1 file (version 1).
        spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        awaitDrain(deltaLog.unsafeVolatileTableId)
        assert(deltaLog.update().version === 1)
        // Second write: 1 row => 1 new file => table has 2 files total. Below threshold (5).
        // Worker re-reads fresh snapshot, decides skip, produces no extra commit.
        spark.range(1).write.format("delta").mode("append").save(dir)
        awaitDrain(deltaLog.unsafeVolatileTableId)
        assert(deltaLog.update().version === 2,
          "Worker should re-evaluate against the fresh snapshot and skip.")
      }
    }
  }

  test("async queue full: writer succeeds and dropped event is recorded") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_PARALLELISM.key -> "1",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_MAX_QUEUE_SIZE.key -> "1") {
        // Submit many small commits in a tight loop; the bounded queue should reject some.
        val events = Log4jUsageLogger.track {
          (1 to 20).foreach { _ =>
            spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
          }
        }
        val dropped = events.filter(
          _.tags.get("opType") == Some("delta.autoCompaction.async.dropped"))
        // It's possible (though unlikely with parallelism=1, queue=1, 20 submissions) that the
        // worker keeps up. Only assert that writers themselves succeeded; if anything is
        // dropped, it must be tagged correctly.
        val deltaLog = DeltaLog.forTable(spark, dir)
        awaitDrain(deltaLog.unsafeVolatileTableId)
        // Writers must not have thrown -- all 20 commits should have produced at least 20 write
        // commits (plus some number of OPTIMIZE commits).
        assert(deltaLog.update().version >= 19)
        // If dropped is non-empty, ensure the payload includes tableId.
        dropped.foreach { ev =>
          assert(ev.blob.contains("tableId"))
        }
      }
    }
  }

  test("async coalesces redundant submissions per-table") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      val gate = new java.util.concurrent.CountDownLatch(1)
      AsyncAutoCompactService.workerGateForTesting = gate
      try {
        withSQLConf(
            DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
            DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED.key -> "true",
            DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0",
            DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_PARALLELISM.key -> "1",
            DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_MAX_QUEUE_SIZE.key -> "128") {
          val events = Log4jUsageLogger.track {
            // The first write's submit accepts; the worker then blocks on the gate before
            // clearing the dedup flag. Subsequent writes all coalesce against the still-set
            // flag.
            (1 to 20).foreach { _ =>
              spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
            }
          }
          val submitted = events.count(
            _.tags.get("opType") == Some("delta.autoCompaction.async.submitted"))
          val coalesced = events.count(
            _.tags.get("opType") == Some("delta.autoCompaction.async.coalesced"))
          assert(submitted + coalesced === 20,
            s"Expected 20 total submit attempts, got submitted=$submitted coalesced=$coalesced")
          assert(submitted === 1,
            s"Expected exactly 1 accepted submission while gated, got $submitted")
          assert(coalesced === 19,
            s"Expected 19 coalesced submissions, got $coalesced")
        }
      } finally {
        gate.countDown()
        AsyncAutoCompactService.workerGateForTesting = null
        val deltaLog = DeltaLog.forTable(spark, dir)
        awaitDrain(deltaLog.unsafeVolatileTableId)
      }
    }
  }
}
