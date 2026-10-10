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

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.hooks.{AsyncAutoCompactService, InflightDMLRegistry}
import org.apache.spark.sql.delta.optimize.CompactionTestHelperForAutoCompaction
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for the sticky inline-fallback behavior.
 *
 * Motivation: a streaming MERGE job will, by design, have each write conflict with the next.
 * If async Auto Compaction yielded on the first conflict and then kept submitting fresh async
 * tasks that yielded again, the table would never receive an OPTIMIZE commit and Spark cycles
 * would be wasted. The fallback mechanism demotes any table that has had a single async yield to "sticky inline"
 * AC for the JVM's lifetime; inline AC commits as the writer's own post-commit hook and cannot
 * be cancelled by a future writer.
 */
class AutoCompactInlineFallbackSuite extends
    CompactionTestHelperForAutoCompaction
  with DeltaSQLCommandTest
  with SharedSparkSession
  with AutoCompactTestUtils {


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

  /** Force exactly one atDequeue yield on the given table by gating the worker and acquiring
   *  DML before releasing the gate. */
  private def forceOneYield(dir: String, tableId: String): Unit = {
    val workerGate = new CountDownLatch(1)
    AsyncAutoCompactService.workerGateForTesting = workerGate
    try {
      InflightDMLRegistry.acquire(tableId)
      try {
        spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
        workerGate.countDown()
        pollUntil {
          assert(
            AsyncAutoCompactService.yieldCountForTesting(tableId, "atDequeue") >= 1,
            "Expected an atDequeue yield to seed the fallback state")
        }
      } finally {
        InflightDMLRegistry.release(tableId)
      }
    } finally {
      AsyncAutoCompactService.workerGateForTesting = null
    }
    awaitDrain(tableId)
  }

  // ---------------------------------------------------------------------------------
  // Core fallback behavior
  // ---------------------------------------------------------------------------------

  test("after a single yield, the same table runs subsequent AC inline") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_FALLBACK_TO_INLINE_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
        // Seed the table.
        spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        val tableId = deltaLog.unsafeVolatileTableId
        awaitDrain(tableId)

        // Force one yield, which marks the table as sticky inline.
        forceOneYield(dir, tableId)
        assert(AsyncAutoCompactService.isInInlineFallback(tableId),
          "Expected the table to be in inline fallback after one yield")

        // Next write should run AC inline -- no new async.submitted event.
        val events = Log4jUsageLogger.track {
          spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
        }
        val submitted = events.count(
          _.tags.get("opType") == Some("delta.autoCompaction.async.submitted"))
        assert(submitted === 0,
          s"Expected no async submission after fallback engaged, got $submitted")
        // The inline AC commit should have landed on the writer's own commit path. Drain just
        // in case anything still sneaked through.
        awaitDrain(tableId)
      }
    }
  }

  test("fallbackEngaged telemetry fires exactly once per table") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_FALLBACK_TO_INLINE_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
        spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        val tableId = deltaLog.unsafeVolatileTableId
        awaitDrain(tableId)

        // First yield must emit fallbackEngaged.
        val firstYieldEvents = Log4jUsageLogger.track {
          forceOneYield(dir, tableId)
        }
        val firstFallback = firstYieldEvents.count(
          _.tags.get("opType") == Some("delta.autoCompaction.async.fallbackEngaged"))
        assert(firstFallback === 1,
          s"Expected fallbackEngaged to fire once on first yield, got $firstFallback")

        // Subsequent yields (force another by directly invoking markInlineFallback via the
        // public yield-counter path) must NOT emit a second fallbackEngaged.
        val secondEvents = Log4jUsageLogger.track {
          // Acquire DML and run a write so the worker dequeues and yields again. Even if
          // it doesn't actually run an async task (because the branch is now inline), we
          // can directly test the idempotency by calling markInlineFallback again.
          AsyncAutoCompactService.markInlineFallback(deltaLog, tableId, "atDequeue")
        }
        val secondFallback = secondEvents.count(
          _.tags.get("opType") == Some("delta.autoCompaction.async.fallbackEngaged"))
        assert(secondFallback === 0,
          s"Expected fallbackEngaged to NOT re-fire (idempotent), got $secondFallback")
      }
    }
  }

  test("fallback is per-table: yielding table A does not affect table B") {
    withTempDir { tempA =>
      withTempDir { tempB =>
        val dirA = tempA.getCanonicalPath
        val dirB = tempB.getCanonicalPath
        withSQLConf(
            DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
            DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED.key -> "true",
            DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_FALLBACK_TO_INLINE_ENABLED.key -> "true",
            DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
          spark.range(10).repartition(5).write.format("delta").mode("append").save(dirA)
          spark.range(10).repartition(5).write.format("delta").mode("append").save(dirB)
          val logA = DeltaLog.forTable(spark, dirA)
          val logB = DeltaLog.forTable(spark, dirB)
          val idA = logA.unsafeVolatileTableId
          val idB = logB.unsafeVolatileTableId
          awaitDrain(idA)
          awaitDrain(idB)

          forceOneYield(dirA, idA)

          assert(AsyncAutoCompactService.isInInlineFallback(idA),
            "Expected table A to be in fallback")
          assert(!AsyncAutoCompactService.isInInlineFallback(idB),
            "Table B must remain in async mode")
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------
  // Config off: behavior unchanged from baseline DML-yield behavior
  // ---------------------------------------------------------------------------------

  test("with fallback disabled, async submission still happens after a yield") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_FALLBACK_TO_INLINE_ENABLED.key -> "false",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
        spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        val tableId = deltaLog.unsafeVolatileTableId
        awaitDrain(tableId)

        forceOneYield(dir, tableId)
        // markInlineFallback was still called (the trigger always fires); only the BRANCH
        // ignores it. Verify by doing a follow-up write with the flag off -- it must still
        // submit async.
        val events = Log4jUsageLogger.track {
          spark.range(10).repartition(5).write.format("delta").mode("append").save(dir)
          awaitDrain(tableId)
        }
        val submitted = events.count(
          _.tags.get("opType") == Some("delta.autoCompaction.async.submitted"))
        assert(submitted >= 1,
          s"With fallback disabled, expected at least one async submission, got $submitted")
      }
    }
  }
}
