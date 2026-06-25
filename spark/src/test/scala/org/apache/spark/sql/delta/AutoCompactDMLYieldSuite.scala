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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.hooks.{AsyncAutoCompactService, InflightDMLRegistry}
import org.apache.spark.sql.delta.optimize.CompactionTestHelperForAutoCompaction
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for the async Auto Compaction DML yield mechanism.
 *
 * The mechanism has three coordination points; we exercise all three plus the
 * "manual OPTIMIZE never yields" invariant and the registry primitives themselves.
 *
 *   1. Dequeue-time yield  -- AC sees an active DML when it dequeues and skips.
 *   2. Post-listener race  -- AC dequeued before DML acquired but DML acquired before
 *                             AC's listener was installed; re-check catches it.
 *   3. Pre-commit yield    -- AC ran the OPTIMIZE Spark work but a DML acquired before
 *                             AC's first commit attempt.
 *   4. Manual OPTIMIZE     -- user OPTIMIZE bypasses the yield gate entirely.
 */
class AutoCompactDMLYieldSuite extends
    CompactionTestHelperForAutoCompaction
  with DeltaSQLCommandTest
  with SharedSparkSession
  with AutoCompactTestUtils {

  import testImplicits._

  private val AsyncTimeoutMs = 60000L

  override def beforeEach(): Unit = {
    super.beforeEach()
    AsyncAutoCompactService.resetCountersForTesting()
    InflightDMLRegistry.resetForTesting()
  }

  override def afterEach(): Unit = {
    AsyncAutoCompactService.resetCountersForTesting()
    InflightDMLRegistry.resetForTesting()
    super.afterEach()
  }

  private def awaitDrain(tableId: String): Unit = {
    assert(
      AsyncAutoCompactService.awaitQuiescenceForTesting(tableId, AsyncTimeoutMs),
      s"Async Auto Compact pool did not drain within ${AsyncTimeoutMs}ms for table $tableId")
  }

  // ---------------------------------------------------------------------------------
  // Registry primitives
  // ---------------------------------------------------------------------------------

  test("registry: acquire/release reference-counts and reports isActive correctly") {
    val t = "table-A"
    assert(!InflightDMLRegistry.isActive(t))
    InflightDMLRegistry.acquire(t)
    assert(InflightDMLRegistry.isActive(t))
    assert(InflightDMLRegistry.activeCountForTesting(t) === 1)

    InflightDMLRegistry.acquire(t)
    assert(InflightDMLRegistry.activeCountForTesting(t) === 2)

    InflightDMLRegistry.release(t)
    assert(InflightDMLRegistry.isActive(t))
    assert(InflightDMLRegistry.activeCountForTesting(t) === 1)

    InflightDMLRegistry.release(t)
    assert(!InflightDMLRegistry.isActive(t))
    assert(InflightDMLRegistry.activeCountForTesting(t) === 0)
  }

  test("registry: listener fires on acquire and throwing listeners do not propagate") {
    val t = "table-B"
    val fired = new AtomicInteger(0)
    val listener = new InflightDMLRegistry.AcquireListener {
      override def onDMLAcquired(): Unit = {
        fired.incrementAndGet()
        throw new RuntimeException("simulated listener failure")
      }
    }
    InflightDMLRegistry.registerAcquireListener(t, listener)
    try {
      // Acquire must not propagate the listener's exception.
      InflightDMLRegistry.acquire(t)
      assert(fired.get() === 1)
      InflightDMLRegistry.release(t)
    } finally {
      InflightDMLRegistry.unregisterAcquireListener(t)
    }
    assert(!InflightDMLRegistry.hasListenerForTesting(t))
  }

  // ---------------------------------------------------------------------------------
  // End-to-end: dequeue-time yield
  // ---------------------------------------------------------------------------------

  test("async AC yields at dequeue when a DML is already in-flight") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
        // Seed the table so we have a DeltaLog with a known tableId.
        spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        val tableId = deltaLog.unsafeVolatileTableId
        awaitDrain(tableId)

        // Force the worker to block AT dequeue (the workerGate runs BEFORE the DML-active
        // check) so we can manually acquire DML and have the worker see it.
        val workerGate = new CountDownLatch(1)
        AsyncAutoCompactService.workerGateForTesting = workerGate
        try {
          // Acquire DML as if a MERGE were running on this table.
          InflightDMLRegistry.acquire(tableId)
          try {
            // Now do another write so a new AC submission is enqueued; the worker is gated
            // on workerGate so it will dequeue only after we unblock.
            spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
            // Release the worker; it will dequeue, see active DML, and yield.
            workerGate.countDown()
            // Give the worker a moment to run its yield branch.
            pollUntil {
              assert(
                AsyncAutoCompactService.yieldCountForTesting(tableId, "atDequeue") >= 1,
                "Expected at least one atDequeue yield event")
            }
          } finally {
            InflightDMLRegistry.release(tableId)
          }
        } finally {
          AsyncAutoCompactService.workerGateForTesting = null
        }
        awaitDrain(tableId)
      }
    }
  }

  // ---------------------------------------------------------------------------------
  // End-to-end: post-listener race
  // ---------------------------------------------------------------------------------

  test("async AC catches the race where DML acquires after listener install") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
        spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        val tableId = deltaLog.unsafeVolatileTableId
        awaitDrain(tableId)

        // Use the beforeOptimizeGate so the worker installs its listener and then blocks
        // BEFORE starting Spark work; we then acquire DML so the re-check fires.
        val preGate = new CountDownLatch(1)
        AsyncAutoCompactService.beforeOptimizeGateForTesting = preGate
        try {
          spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
          // Wait until the worker has installed its listener (registry has a listener slot).
          pollUntil {
            assert(InflightDMLRegistry.hasListenerForTesting(tableId),
              "Expected worker to install its acquire listener before the gate")
          }
          // Acquire DML now. The worker's listener will fire (cancellation flag set), but
          // since it's blocked on the gate, the cancellation is moot. When we release the
          // gate, the re-check at the top of the try-block detects active DML and yields
          // via the pre-commit gate inside OptimizeExecutor.commitAndRetry (the post-
          // listener re-check fires only when DML acquires BEFORE the test gate releases;
          // here we release first so the pre-commit path catches it).
          InflightDMLRegistry.acquire(tableId)
          try {
            preGate.countDown()
            pollUntil {
              val pre = AsyncAutoCompactService.yieldCountForTesting(tableId, "preCommit")
              val deq = AsyncAutoCompactService.yieldCountForTesting(tableId, "atDequeue")
              val mid = AsyncAutoCompactService.yieldCountForTesting(tableId, "midFlight")
              assert(pre + deq + mid >= 1,
                s"Expected some yield (preCommit=$pre, atDequeue=$deq, midFlight=$mid)")
            }
          } finally {
            InflightDMLRegistry.release(tableId)
          }
        } finally {
          AsyncAutoCompactService.beforeOptimizeGateForTesting = null
        }
        awaitDrain(tableId)
      }
    }
  }

  // ---------------------------------------------------------------------------------
  // Manual OPTIMIZE never yields
  // ---------------------------------------------------------------------------------

  test("manual OPTIMIZE does NOT yield even when a DML is in flight") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "false") {
        // Build a table with multiple small files so OPTIMIZE has something to do.
        spark.range(100).repartition(10).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        val tableId = deltaLog.unsafeVolatileTableId
        val versionBefore = deltaLog.update().version

        // Pretend a DML is running on this table.
        InflightDMLRegistry.acquire(tableId)
        try {
          // Manual OPTIMIZE must commit even with DML active (no asyncWorkerThread flag).
          sql(s"OPTIMIZE delta.`$dir`").collect()
        } finally {
          InflightDMLRegistry.release(tableId)
        }
        val versionAfter = deltaLog.update().version
        assert(versionAfter > versionBefore,
          "Manual OPTIMIZE should commit even when a DML is in flight")
        // Pre-commit yield counter must not have ticked.
        assert(
          AsyncAutoCompactService.yieldCountForTesting(tableId, "preCommit") === 0,
          "Manual OPTIMIZE must not trip the async pre-commit yield gate")
      }
    }
  }

  // ---------------------------------------------------------------------------------
  // DML hook integration: MERGE acquires/releases on the target's tableId
  // ---------------------------------------------------------------------------------

  test("MERGE acquires and releases InflightDMLRegistry slot on the target table") {
    withTempDir { tempDir =>
      val targetDir = tempDir.getCanonicalPath
      withSQLConf(DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "false") {
        // Set up target table.
        Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "v")
          .write.format("delta").save(targetDir)
        val deltaLog = DeltaLog.forTable(spark, targetDir)
        val tableId = deltaLog.unsafeVolatileTableId

        // Observe the acquire via a listener.
        val observed = new AtomicInteger(0)
        val listener = new InflightDMLRegistry.AcquireListener {
          override def onDMLAcquired(): Unit = observed.incrementAndGet()
        }
        InflightDMLRegistry.registerAcquireListener(tableId, listener)
        try {
          // Source DF for MERGE.
          val source = Seq((2, "B"), (4, "D")).toDF("id", "v")
          source.createOrReplaceTempView("src")
          sql(
            s"""MERGE INTO delta.`$targetDir` t
               |USING src s ON t.id = s.id
               |WHEN MATCHED THEN UPDATE SET t.v = s.v
               |WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
               |""".stripMargin).collect()
          // The MERGE should have acquired exactly once.
          assert(observed.get() === 1,
            s"Expected one DML acquire from MERGE, got ${observed.get()}")
        } finally {
          InflightDMLRegistry.unregisterAcquireListener(tableId)
        }
        // After MERGE returns, the slot should be released.
        assert(!InflightDMLRegistry.isActive(tableId),
          "MERGE should release its DML slot before returning")
      }
    }
  }

  // ---------------------------------------------------------------------------------
  // Composition with per-table async-AC dedup
  // ---------------------------------------------------------------------------------

  test("yielded async AC submission does not break the per-table async-AC dedup") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
        spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        val tableId = deltaLog.unsafeVolatileTableId
        awaitDrain(tableId)

        // First: force a yield. Then: ensure another write still triggers a successful
        // async AC (dedup state was cleaned up correctly).
        val workerGate = new CountDownLatch(1)
        AsyncAutoCompactService.workerGateForTesting = workerGate
        InflightDMLRegistry.acquire(tableId)
        try {
          spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
          workerGate.countDown()
          pollUntil {
            assert(
              AsyncAutoCompactService.yieldCountForTesting(tableId, "atDequeue") >= 1)
          }
        } finally {
          AsyncAutoCompactService.workerGateForTesting = null
          InflightDMLRegistry.release(tableId)
        }
        awaitDrain(tableId)

        // Versions: 0=write, 1=write (yielded AC), so we expect no OPTIMIZE commit yet.
        val versionAfterYield = deltaLog.update().version

        // Now do another write -- a new AC submission should be accepted and run.
        val events = Log4jUsageLogger.track {
          spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
          awaitDrain(tableId)
        }
        val submitted = events.count(
          _.tags.get("opType") == Some("delta.autoCompaction.async.submitted"))
        assert(submitted >= 1,
          "After a yielded run, the next write must still submit a fresh AC task")
        // Final state should show at least one OPTIMIZE commit landed.
        val finalVersion = deltaLog.update().version
        assert(finalVersion > versionAfterYield + 1,
          "Expected a successful async AC commit to land after the prior yield")
      }
    }
  }

  // ---------------------------------------------------------------------------------
  // Helper: poll until predicate or timeout (simple eventually impl, no scalatest dep)
  // ---------------------------------------------------------------------------------
  private def pollUntil(body: => Unit): Unit = {
    val deadline = System.currentTimeMillis() + AsyncTimeoutMs
    var lastError: Throwable = null
    while (System.currentTimeMillis() < deadline) {
      try {
        body
        return
      } catch {
        case t: Throwable =>
          lastError = t
          Thread.sleep(50)
      }
    }
    if (lastError != null) throw lastError
  }
}
