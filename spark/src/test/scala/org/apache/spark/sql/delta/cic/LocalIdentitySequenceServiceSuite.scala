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

package org.apache.spark.sql.delta.cic

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, TimeUnit}

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for the in-memory backend used by tests that don't need to round-trip
 * through a real UC. The caller supplies the `sequenceId` (a unique id); these tests use
 * stable string ids for determinism.
 */
class LocalIdentitySequenceServiceSuite extends SparkFunSuite {

  private def newService(): LocalIdentitySequenceService = new LocalIdentitySequenceService()

  private def create(
      seqService: LocalIdentitySequenceService,
      sequenceId: String,
      start: Long,
      step: Long): Unit =
    seqService.createSequence(CreateSequenceRequest(
      sequenceId = sequenceId, tableId = "tbl", start = start, step = step))

  test("create registers the supplied id and reserve produces the expected range") {
    val seqService = newService()
    create(seqService, "seq-1", start = 1L, step = 1L)
    assert(seqService.hasSequence(tableId = "tbl", sequenceId = "seq-1"))

    val resp = seqService.reserveIds(
      ReserveIdsRequest("seq-1", tableId = "tbl", count = 5L, step = 1L))
    assert(resp.sequenceId === "seq-1")
    assert(resp.rangeStart === 1L)
    assert(resp.rangeEnd === 5L)
    assert(resp.step === 1L)
  }

  test("successive reservations return non-overlapping ranges") {
    val seqService = newService()
    create(seqService, "seq", start = 1L, step = 1L)
    val r1 =
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 100L, step = 1L))
    val r2 =
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 100L, step = 1L))
    val r3 =
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 50L, step = 1L))
    assert(r1.rangeEnd < r2.rangeStart, s"r1=$r1 r2=$r2")
    assert(r2.rangeEnd < r3.rangeStart, s"r2=$r2 r3=$r3")
    assert(r1.rangeStart === 1L && r1.rangeEnd === 100L)
    assert(r2.rangeStart === 101L && r2.rangeEnd === 200L)
    assert(r3.rangeStart === 201L && r3.rangeEnd === 250L)
  }

  test("reserve with step greater than 1 strides correctly") {
    val seqService = newService()
    create(seqService, "seq", start = 0L, step = 10L)
    val resp =
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 3L, step = 10L))
    // Values: 0, 10, 20.
    assert(resp.rangeStart === 0L)
    assert(resp.rangeEnd === 20L)
    assert(resp.step === 10L)
  }

  test("reserve with negative step produces a descending range") {
    val seqService = newService()
    create(seqService, "seq", start = 100L, step = -1L)
    val r1 =
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 5L, step = -1L))
    // Values: 100, 99, 98, 97, 96.
    assert(r1.rangeStart === 100L && r1.rangeEnd === 96L && r1.step === -1L)
    val r2 =
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 5L, step = -1L))
    // Subsequent reservation continues from 95, ending at 91.
    assert(r2.rangeStart === 95L && r2.rangeEnd === 91L)
  }

  test("reserve with count = 1 collapses range to a single value") {
    val seqService = newService()
    create(seqService, "seq", start = 42L, step = 7L)
    val resp =
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 1L, step = 7L))
    assert(resp.rangeStart === 42L)
    assert(resp.rangeEnd === 42L)
    assert(resp.step === 7L)
  }

  test("reserve from nonexistent sequence fails") {
    val seqService = newService()
    val ex = intercept[NoSuchElementException] {
      seqService.reserveIds(ReserveIdsRequest(
        sequenceId = "does-not-exist", tableId = "tbl", count = 1L, step = 1L))
    }
    assert(ex.getMessage.contains("does-not-exist"))
  }

  test("reserve under a different tableId than the create is a not-found") {
    // (tableId, sequenceId) names the counter, mirroring the UC contract: a reserve that carries
    // the wrong table scope (e.g. a table's own metadata.id where registration used the UC table
    // id) must surface as a not-found, never silently hit another table's counter.
    val seqService = newService()
    create(seqService, "seq", start = 1L, step = 1L) // registers under tableId = "tbl"
    val ex = intercept[NoSuchElementException] {
      seqService.reserveIds(
        ReserveIdsRequest("seq", tableId = "other-table", count = 1L, step = 1L))
    }
    assert(ex.getMessage.contains("other-table"))
    // The correctly-scoped reserve still works and starts at `start`.
    val resp =
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 1L, step = 1L))
    assert(resp.rangeStart === 1L)
  }

  test("reserve with a step that differs from the registered step is rejected") {
    // Request step and registered step share a source of truth (the column schema); a
    // mismatch means the caller's schema drifted from what registration recorded.
    val seqService = newService()
    create(seqService, "seq", start = 1L, step = 2L)
    val ex = intercept[IllegalStateException] {
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 1L, step = 3L))
    }
    assert(ex.getMessage.contains("does not match the registered step"))
  }

  test("the same sequenceId under two tables names two independent counters") {
    // The UC clone path replicates a sequence id into the clone's table scope with its own
    // counter state; the stub mirrors that keying.
    val seqService = newService()
    seqService.createSequence(CreateSequenceRequest(
      sequenceId = "seq", tableId = "table-a", start = 1L, step = 1L))
    seqService.createSequence(CreateSequenceRequest(
      sequenceId = "seq", tableId = "table-b", start = 100L, step = 1L))
    val ra =
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "table-a", count = 5L, step = 1L))
    val rb =
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "table-b", count = 5L, step = 1L))
    assert(ra.rangeStart === 1L && ra.rangeEnd === 5L)
    assert(rb.rangeStart === 100L && rb.rangeEnd === 104L, s"independent counter expected, got $rb")
  }

  test("create with empty tableId is rejected") {
    val seqService = newService()
    intercept[IllegalArgumentException] {
      seqService.createSequence(CreateSequenceRequest(
        sequenceId = "seq", tableId = "", start = 1L, step = 1L))
    }
  }

  test("create with step zero is rejected") {
    val seqService = newService()
    intercept[IllegalArgumentException] {
      create(seqService, "seq", start = 1L, step = 0L)
    }
  }

  test("create with empty sequenceId is rejected") {
    val seqService = newService()
    intercept[IllegalArgumentException] {
      create(seqService, "", start = 1L, step = 1L)
    }
  }

  test("createSequence is idempotent for a matching repeat (CreateOrGet)") {
    val seqService = newService()
    create(seqService, "seq", start = 1L, step = 1L)
    val r1 = seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 5L, step = 1L))
    assert(r1.rangeStart === 1L && r1.rangeEnd === 5L)
    // A repeat create with matching start/step is a no-op; it must NOT reset the
    // live counter (this is the driver's crash-recovery re-issue path).
    create(seqService, "seq", start = 1L, step = 1L)
    val r2 = seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 5L, step = 1L))
    assert(r2.rangeStart === 6L && r2.rangeEnd === 10L, s"counter must not reset, got $r2")
  }

  test("createSequence rejects a repeat with mismatched start/step") {
    val seqService = newService()
    create(seqService, "seq", start = 1L, step = 1L)
    intercept[IllegalStateException] {
      create(seqService, "seq", start = 2L, step = 1L)
    }
    intercept[IllegalStateException] {
      create(seqService, "seq", start = 1L, step = 5L)
    }
  }

  test("reserve with non-positive count is rejected") {
    val seqService = newService()
    create(seqService, "seq", start = 1L, step = 1L)
    intercept[IllegalArgumentException] {
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 0L, step = 1L))
    }
    intercept[IllegalArgumentException] {
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = -3L, step = 1L))
    }
  }

  test("reset clears state") {
    val seqService = newService()
    create(seqService, "seq", start = 1L, step = 1L)
    seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 10L, step = 1L))
    seqService.reset()

    // After reset, the sequence is no longer present.
    intercept[NoSuchElementException] {
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 1L, step = 1L))
    }
    // And the id can be re-created from scratch, starting fresh.
    create(seqService, "seq", start = 1L, step = 1L)
    val resp =
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 1L, step = 1L))
    assert(resp.rangeStart === 1L && resp.rangeEnd === 1L)
  }

  test("reserve that would overflow the sequence is rejected") {
    // Two overflow scenarios. Both must be rejected without advancing `current`.
    //
    // Case A: overflow before getAndUpdate (stride = step * count overflows).
    // With bigStep * 8, Math.multiplyExact throws at the stride pre-check on the
    // calling thread -- the getAndUpdate lambda is never entered.
    val seqService = newService()
    val bigStep = Long.MaxValue / 4L
    create(seqService, "seq", start = 0L, step = bigStep)
    intercept[ArithmeticException] {
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 8L, step = bigStep))
    }
    val okA =
      seqService.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 2L, step = bigStep))
    assert(okA.rangeStart === 0L && okA.rangeEnd === bigStep,
      "overflow before getAndUpdate must not advance current")

    // Case B: overflow inside getAndUpdate (stride/endOffset are fine, but
    // addExact(c, stride) overflows when c is near Long.MaxValue). This exercises
    // the CAS-rollback path: the lambda throws, the CAS does not commit, and
    // `current` must remain at the pre-attempt value.
    val seqService2 = newService()
    create(seqService2, "seq", start = Long.MaxValue - 2L, step = 1L)
    intercept[ArithmeticException] {
      seqService2.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 3L, step = 1L))
    }
    val okB =
      seqService2.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 1L, step = 1L))
    assert(okB.rangeStart === Long.MaxValue - 2L && okB.rangeEnd === Long.MaxValue - 2L,
      "overflow inside getAndUpdate (CAS rollback) must not advance current")

    // Case C: descending (negative step) underflow mirror of Case B.
    val seqService3 = newService()
    create(seqService3, "seq", start = Long.MinValue + 2L, step = -1L)
    intercept[ArithmeticException] {
      seqService3.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 3L, step = -1L))
    }
    val okC =
      seqService3.reserveIds(ReserveIdsRequest("seq", tableId = "tbl", count = 1L, step = -1L))
    assert(okC.rangeStart === Long.MinValue + 2L && okC.rangeEnd === Long.MinValue + 2L,
      "underflow inside getAndUpdate (CAS rollback, descending) must not advance current")
  }

  test("concurrent reservations from many threads produce non-overlapping ranges") {
    val seqService = newService()
    create(seqService, "seq", start = 1L, step = 1L)
    val numThreads = 16
    val reservesPerThread = 50
    val reserveCount = 100L
    // scalastyle:off sparkThreadPools
    val pool = Executors.newFixedThreadPool(numThreads)
    // scalastyle:on sparkThreadPools
    val startGate = new CountDownLatch(1)
    // Track every observed value across all threads, duplicates would mean ranges overlap.
    val seen = new ConcurrentHashMap[Long, java.lang.Boolean]()
    try {
      val futures = (1 to numThreads).map { _ =>
        pool.submit(new Runnable {
          override def run(): Unit = {
            startGate.await()
            (1 to reservesPerThread).foreach { _ =>
              val resp =
                seqService.reserveIds(
                  ReserveIdsRequest("seq", tableId = "tbl", count = reserveCount, step = 1L))
              (resp.rangeStart to resp.rangeEnd).foreach { v =>
                val prior = seen.putIfAbsent(v, java.lang.Boolean.TRUE)
                assert(prior == null, s"duplicate identity value emitted: $v")
              }
            }
          }
        })
      }
      startGate.countDown()
      // Future.get rethrows worker-thread assertion failures (as ExecutionException);
      // otherwise a failure is swallowed and only shows as a size mismatch below.
      futures.foreach(_.get(30L, TimeUnit.SECONDS))
    } finally {
      pool.shutdownNow()
    }
    val expectedCount = numThreads.toLong * reservesPerThread * reserveCount
    assert(
      seen.size() === expectedCount,
      s"expected $expectedCount distinct values, got ${seen.size()}")
    // With size == expectedCount and unique contiguous ranges from 1, the values
    // are exactly [1, expectedCount]; endpoint checks are O(1) (vs scanning min/max).
    assert(seen.containsKey(1L))
    assert(seen.containsKey(expectedCount))
  }
}
