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

package org.apache.spark.sql.delta.hooks

import java.util.UUID
import java.util.concurrent.{
  ConcurrentHashMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit
}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.CommittedTransaction
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkException
import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.ThreadUtils

/**
 * Marker exception thrown from inside [[org.apache.spark.sql.delta.commands.OptimizeExecutor]]
 * when an async Auto Compaction batch is asked to yield to an in-flight DML on the same table
 * (see [[InflightDMLRegistry]]). Caught by [[AsyncAutoCompactService.AsyncAutoCompactTask]]
 * and converted into a `yieldedToDML.preCommit` telemetry event + WARN log; never propagated
 * to user code.
 *
 * Distinguished from generic interrupts so the async worker can tell "DML asked us to yield"
 * apart from "something else went wrong".
 */
private[delta] class AsyncAutoCompactCancelledException(val tableId: String, msg: String)
  extends RuntimeException(msg)

/**
 * JVM-wide singleton that executes Auto Compaction off the writer thread.
 *
 * When [[DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED]] is set, [[AutoCompactBase.run]] hands
 * the committed transaction to this service instead of running OPTIMIZE inline. The writer's
 * post-commit hook returns immediately; the OPTIMIZE work is performed on a daemon worker
 * thread and lands as its own commit shortly after.
 *
 * Correctness invariants:
 *   - Workers re-evaluate eligibility against a freshly-loaded snapshot (`deltaLog.update()`).
 *     The enqueue-time `postCommitSnapshot` can be minutes stale by the time the worker runs;
 *     a concurrent writer (this JVM or another) may have already compacted.
 *   - Eligibility, partition reservation (`AutoCompactPartitionReserve`), and OPTIMIZE all
 *     happen on the same worker thread. The reservation window therefore covers the OPTIMIZE
 *     call exactly as in inline mode.
 *   - OPTIMIZE-vs-OPTIMIZE and OPTIMIZE-vs-write conflicts use the same `commitAndRetry` loop
 *     in `OptimizeExecutor` that inline Auto Compaction relies on. Async introduces no new
 *     conflict class.
 *   - The worker sets [[SparkSession.setActiveSession]] / [[SparkSession.setDefaultSession]]
 *     on entry and clears them in `finally`. Several delta-spark internals consult
 *     `SparkSession.getActiveSession` (including `OptimizeExecutor`'s codegen path).
 *
 * DML yielding (see [[InflightDMLRegistry]] and PR D in plan.md):
 *   - At dequeue, if a long-running DML is in flight on the target table, the task exits
 *     silently with a WARN log -- the next user write's post-commit hook will resubmit.
 *   - During execution, if a DML acquires the registry on the target table, a listener fires
 *     `SparkContext.cancelJobGroup` to interrupt our in-flight Spark stages. The resulting
 *     `SparkException` is caught and converted to a WARN log + telemetry event.
 *   - At commit time, `OptimizeExecutor.commitAndRetry` checks the registry one last time;
 *     if a DML acquired during the millisecond gap between Spark-job-end and commit, it
 *     throws [[AsyncAutoCompactCancelledException]] which we catch here.
 *   - In all three cases, the partial Spark work AC had already done becomes garbage that
 *     `VACUUM` reaps based on its normal retention policy.
 */
private[delta] object AsyncAutoCompactService extends DeltaLogging {

  /** Telemetry event types. */
  private val EV_SUBMITTED = "delta.autoCompaction.async.submitted"
  private val EV_DROPPED = "delta.autoCompaction.async.dropped"
  private val EV_COALESCED = "delta.autoCompaction.async.coalesced"
  private val EV_ERROR = "delta.autoCompaction.async.error"
  private val EV_COMPLETED = "delta.autoCompaction.async.completed"
  private val EV_YIELDED_DEQUEUE = "delta.autoCompaction.async.yieldedToDML.atDequeue"
  private val EV_YIELDED_MIDFLIGHT = "delta.autoCompaction.async.yieldedToDML.midFlight"
  private val EV_YIELDED_PRECOMMIT = "delta.autoCompaction.async.yieldedToDML.preCommit"

  /**
   * Per-table inflight count (queued + actively running). Used by tests
   * (`awaitQuiescenceForTesting`) and telemetry payloads.
   *
   * Key is `deltaLog.unsafeVolatileTableId`. Incremented on accepted submission, decremented
   * in the worker's finally block. With per-table dedup (see `queuedByTable`) this counter
   * tops out at 2 per table (one running + one queued).
   */
  private val inflightByTable = new ConcurrentHashMap[String, AtomicInteger]()

  /**
   * Per-table "submission already queued" flag. Drives dedup: while a submission for a given
   * table is sitting in the pool queue, additional submissions are dropped as redundant --
   * the queued worker re-reads `deltaLog.update()` at execution time, so it will pick up any
   * commits that landed after enqueue. The flag is cleared at the start of `run()` (not at
   * end), so a new submission arriving while the worker is actively running gets queued (1
   * running + 1 queued max per table).
   */
  private val queuedByTable = new ConcurrentHashMap[String, AtomicBoolean]()

  /**
   * Thread-local "I am an async Auto Compaction worker" flag. Set true at the top of
   * [[AsyncAutoCompactTask.run]] and cleared in the matching `finally`. Consumed by
   * `OptimizeExecutor.commitAndRetry` to decide whether to apply the pre-commit DML yield
   * check -- inline Auto Compaction and user-initiated OPTIMIZE both leave it false and
   * therefore never yield (a manual `OPTIMIZE` is an explicit user action and must run).
   */
  private val asyncWorkerThread = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }

  /** True iff the current thread is an async Auto Compaction worker. */
  def isAsyncWorker: Boolean = asyncWorkerThread.get()

  /**
   * The shared, bounded, daemon thread pool.
   *
   * Lazy so we don't allocate threads until something actually opts in. Configured from the
   * first `submit` call's `SparkSession`; subsequent sessions reuse the pool -- this is a
   * deliberate simplification (matching the JVM-wide nature of the existing
   * `AutoCompactPartitionReserve` singleton). Re-configuration at runtime is not supported in
   * this version; restart the JVM to change pool sizing.
   */
  @volatile private var poolOpt: Option[ThreadPoolExecutor] = None

  /** True iff async submission is enabled in this session. */
  def isEnabled(spark: SparkSession): Boolean =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_ENABLED)

  /** Test/observability hook. */
  def pendingForTable(tableId: String): Int = {
    val ai = inflightByTable.get(tableId)
    if (ai == null) 0 else ai.get()
  }

  /**
   * Lazily initialize the worker pool. Idempotent and safe under concurrent first-call.
   *
   * Capacity is read from the first caller's `SparkSession`. This matches the lifetime of the
   * existing JVM-wide singletons (`AutoCompactPartitionReserve`, `AutoCompactPartitionStats`)
   * and is acceptable because pool sizing is operational and JVM-wide regardless of which
   * session opted in first.
   */
  private def ensurePool(spark: SparkSession): ThreadPoolExecutor = {
    poolOpt match {
      case Some(p) => p
      case None => initPool(spark)
    }
  }

  private def initPool(spark: SparkSession): ThreadPoolExecutor = synchronized {
    poolOpt.getOrElse {
      val conf = spark.sessionState.conf
      val parallelism = conf.getConf(DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_PARALLELISM)
      val maxQueueSize = conf.getConf(DeltaSQLConf.DELTA_AUTO_COMPACT_ASYNC_MAX_QUEUE_SIZE)
      val queue = new LinkedBlockingQueue[Runnable](maxQueueSize)
      val factory = ThreadUtils.namedThreadFactory("delta-auto-compact-async")
      val pool = new ThreadPoolExecutor(
        parallelism, parallelism,
        60L, TimeUnit.SECONDS,
        queue,
        factory)
      pool.allowCoreThreadTimeOut(true)
      poolOpt = Some(pool)
      pool
    }
  }

  /**
   * Submit a CommittedTransaction for asynchronous Auto Compaction.
   *
   * Returns immediately. The worker may run minutes later (queue-depth dependent) and
   * re-evaluates eligibility on a fresh snapshot at that point.
   *
   * Backpressure (queue full): records a `dropped` event and returns. No exception is
   * raised to the writer -- the writer is intentionally insulated from async-mode failures.
   */
  def submit(spark: SparkSession, txn: CommittedTransaction): Unit = {
    val pool = ensurePool(spark)
    val deltaLog = txn.deltaLog
    val tableId = deltaLog.unsafeVolatileTableId

    // Dedup: if a submission is already queued for this table, drop this one as redundant.
    // The queued worker re-reads `deltaLog.update()` at execution time and will absorb any
    // commits that landed between the earlier submit and that re-read, so we don't lose
    // eligibility for those commits. This prevents queue churn under high-throughput writes
    // to the same table (which would otherwise produce N submissions, N-1 of which would
    // wake up only to no-op via AutoCompactPartitionReserve).
    val queued = queuedByTable.computeIfAbsent(tableId, _ => new AtomicBoolean(false))
    if (!queued.compareAndSet(false, true)) {
      recordDeltaEvent(deltaLog, EV_COALESCED, data = Map(
        "tableId" -> tableId,
        "queueSize" -> pool.getQueue.size
      ))
      return
    }

    val inflight = inflightByTable.computeIfAbsent(tableId, _ => new AtomicInteger(0))
    inflight.incrementAndGet()

    // Heap mitigation: the queued task can sit in the bounded queue for a long time when the
    // pool is saturated. Strip the large `committedActions` payload (potentially thousands of
    // AddFile actions per commit) before enqueueing -- the downstream code path
    // (compactIfNecessary / prepareAutoCompactRequest) does not consult committedActions. We
    // also drop postCommitHooks because the worker doesn't re-run them.
    val lightTxn = txn.copy(
      committedActions = Nil,
      postCommitHooks = Nil)

    val task: Runnable = new AsyncAutoCompactTask(spark, lightTxn, tableId)
    try {
      pool.execute(task)
      recordDeltaEvent(deltaLog, EV_SUBMITTED, data = Map(
        "tableId" -> tableId,
        "queueSize" -> pool.getQueue.size,
        "pendingForTable" -> inflight.get
      ))
    } catch {
      case _: java.util.concurrent.RejectedExecutionException =>
        inflight.decrementAndGet()
        queued.set(false)
        recordDeltaEvent(deltaLog, EV_DROPPED, data = Map(
          "tableId" -> tableId,
          "queueSize" -> pool.getQueue.size,
          "reason" -> "queue full"
        ))
        logInfo(log"Async Auto Compaction queue full; dropping submission for table " +
          log"${MDC(DeltaLogKeys.METADATA_ID, tableId)}")
    }
  }

  /** Visible for testing. Awaits the pool draining; returns true if it drained in time. */
  private[delta] def awaitQuiescenceForTesting(
      tableId: String, timeoutMs: Long): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (pendingForTable(tableId) > 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(20)
    }
    pendingForTable(tableId) == 0
  }

  /** Visible for testing. Resets per-table inflight counters. Does NOT shut down the pool. */
  private[delta] def resetCountersForTesting(): Unit = {
    inflightByTable.clear()
    queuedByTable.clear()
    workerGateForTesting = null
    yieldCountersForTesting.clear()
  }

  /**
   * Visible for testing. When set, the worker awaits this latch BEFORE clearing the per-table
   * dedup flag and BEFORE doing any work. Lets tests force a known overlap window during which
   * additional submissions are guaranteed to coalesce.
   */
  @volatile private[delta] var workerGateForTesting: java.util.concurrent.CountDownLatch = null

  /**
   * Visible for testing. When set, the worker awaits this latch AFTER setting up the
   * cancellation listener but BEFORE starting the OPTIMIZE Spark job. Used by the mid-flight
   * cancellation test to deterministically interleave: worker -> install listener -> block;
   * DML -> acquire (fires listener); worker -> proceed (sees cancelled flag set).
   */
  @volatile private[delta] var beforeOptimizeGateForTesting:
    java.util.concurrent.CountDownLatch = null

  /**
   * Visible for testing. Per-table count of each yield kind, so tests can assert exact
   * yield-path coverage without log scraping. Keys: "atDequeue", "midFlight", "preCommit".
   */
  private[delta] val yieldCountersForTesting =
    new ConcurrentHashMap[String, ConcurrentHashMap[String, AtomicInteger]]()

  private def incYieldCounter(tableId: String, kind: String): Unit = {
    val perTable = yieldCountersForTesting.computeIfAbsent(
      tableId, _ => new ConcurrentHashMap[String, AtomicInteger]())
    perTable.computeIfAbsent(kind, _ => new AtomicInteger(0)).incrementAndGet()
  }

  private[delta] def yieldCountForTesting(tableId: String, kind: String): Int = {
    val perTable = yieldCountersForTesting.get(tableId)
    if (perTable == null) 0
    else {
      val ai = perTable.get(kind)
      if (ai == null) 0 else ai.get()
    }
  }

  /**
   * Worker task. Defined as an inner class so it sees `inflightByTable` and the EV_* constants
   * without leaking them publicly.
   */
  private class AsyncAutoCompactTask(
      spark: SparkSession,
      txn: CommittedTransaction,
      tableId: String) extends Runnable {

    override def run(): Unit = {
      // Test hook: block before clearing the dedup flag so tests can verify coalescing.
      val gate = workerGateForTesting
      if (gate != null) {
        gate.await(30, TimeUnit.SECONDS)
      }
      // Clear the dedup flag NOW (before doing work). This way a new submission arriving while
      // we run gets accepted and queued; that worker will then re-read the latest snapshot and
      // pick up any commits that happen during our execution. Cap is 1 running + 1 queued.
      val queued = queuedByTable.get(tableId)
      if (queued != null) queued.set(false)

      // --- DML yield check #1: at dequeue ---
      // If a long-running DML is already in flight on this table, skip silently. The DML's
      // own post-commit hook will resubmit AC when it releases. No work has been wasted.
      if (InflightDMLRegistry.isActive(tableId)) {
        logWarning(log"Async Auto Compaction yielded to in-flight DML at dequeue for table " +
          log"${MDC(DeltaLogKeys.METADATA_ID, tableId)}; skipping this OPTIMIZE attempt.")
        recordDeltaEvent(txn.deltaLog, EV_YIELDED_DEQUEUE, data = Map("tableId" -> tableId))
        incYieldCounter(tableId, "atDequeue")
        decrementInflight()
        return
      }

      // Re-establish thread-locals consulted by OptimizeExecutor / DeltaUDF / Checksum / etc.
      SparkSession.setActiveSession(spark)
      SparkSession.setDefaultSession(spark)
      asyncWorkerThread.set(true)

      // --- DML yield instrumentation: install listener that interrupts our Spark stages if
      // a DML acquires while we're mid-flight. The listener calls cancelJobGroup which makes
      // Spark deliver a TaskKilledException to in-flight executor tasks; on the driver side
      // it surfaces as a SparkException that we catch below.
      val cancelled = new AtomicBoolean(false)
      val jobGroupId = s"async-auto-compact-${UUID.randomUUID()}"
      val listener = new InflightDMLRegistry.AcquireListener {
        override def onDMLAcquired(): Unit = {
          if (cancelled.compareAndSet(false, true)) {
            spark.sparkContext.cancelJobGroup(jobGroupId)
          }
        }
      }
      InflightDMLRegistry.registerAcquireListener(tableId, listener)

      try {
        // Catch the race where DML acquires between our dequeue check above and our listener
        // installation just now. Without this, the listener wouldn't fire (DML already past
        // its acquire call) and we'd proceed only to fail at the pre-commit gate.
        if (InflightDMLRegistry.isActive(tableId)) {
          logWarning(log"Async Auto Compaction yielded to in-flight DML at dequeue for table " +
            log"${MDC(DeltaLogKeys.METADATA_ID, tableId)} (post-listener race).")
          recordDeltaEvent(txn.deltaLog, EV_YIELDED_DEQUEUE, data = Map("tableId" -> tableId))
          incYieldCounter(tableId, "atDequeue")
          return
        }

        // Test hook: block here after listener is installed but before Spark work starts.
        val pre = beforeOptimizeGateForTesting
        if (pre != null) {
          pre.await(30, TimeUnit.SECONDS)
        }

        // Tag all Spark jobs launched by this OPTIMIZE so cancelJobGroup can find them.
        // interruptOnCancel=true asks Spark to send Thread.interrupt to executor task threads.
        spark.sparkContext.setJobGroup(
          jobGroupId,
          s"Async Auto Compaction for $tableId",
          interruptOnCancel = true)

        try {
          // Re-read latest state: a concurrent writer may have already compacted, or more
          // small files may have accumulated. Eligibility is evaluated against this fresh
          // snapshot.
          val freshSnapshot = txn.deltaLog.update(catalogTableOpt = txn.catalogTable)
          val freshTxn = txn.copy(postCommitSnapshot = freshSnapshot)
          AutoCompact.compactIfNecessary(spark, freshTxn, AutoCompact.OP_TYPE + ".async", None)
          recordDeltaEvent(txn.deltaLog, EV_COMPLETED, data = Map("tableId" -> tableId))
        } finally {
          spark.sparkContext.clearJobGroup()
        }
      } catch {
        case _: AsyncAutoCompactCancelledException =>
          // Pre-commit gate fired inside OptimizeExecutor.commitAndRetry.
          logWarning(log"Async Auto Compaction aborted at commit boundary due to in-flight " +
            log"DML for table ${MDC(DeltaLogKeys.METADATA_ID, tableId)}.")
          recordDeltaEvent(txn.deltaLog, EV_YIELDED_PRECOMMIT,
            data = Map("tableId" -> tableId))
          incYieldCounter(tableId, "preCommit")

        case e: SparkException if cancelled.get() =>
          // Mid-flight: DML acquired while a Spark stage was running, listener fired
          // cancelJobGroup, Spark surfaced the cancellation up to us as a SparkException.
          logWarning(log"Async Auto Compaction cancelled mid-flight due to in-flight DML " +
            log"for table ${MDC(DeltaLogKeys.METADATA_ID, tableId)}; partial Spark work " +
            log"aborted, any orphaned files will become VACUUM candidates on the next sweep.")
          recordDeltaEvent(txn.deltaLog, EV_YIELDED_MIDFLIGHT, data = Map(
            "tableId" -> tableId,
            "exception" -> e.getClass.getName))
          incYieldCounter(tableId, "midFlight")

        case e: InterruptedException if cancelled.get() =>
          // Same scenario as above but on a code path where InterruptedException bubbles
          // directly (e.g. while sleeping in a retry loop). Treat identically.
          logWarning(log"Async Auto Compaction cancelled mid-flight due to in-flight DML " +
            log"for table ${MDC(DeltaLogKeys.METADATA_ID, tableId)}; interrupted before " +
            log"Spark work could complete.")
          recordDeltaEvent(txn.deltaLog, EV_YIELDED_MIDFLIGHT, data = Map(
            "tableId" -> tableId,
            "exception" -> e.getClass.getName))
          incYieldCounter(tableId, "midFlight")
          Thread.currentThread().interrupt() // preserve interrupt status

        case NonFatal(e) =>
          logWarning(log"Async Auto Compaction failed for table " +
            log"${MDC(DeltaLogKeys.METADATA_ID, tableId)}: " +
            log"${MDC(DeltaLogKeys.ERROR, e.getMessage)}")
          recordDeltaEvent(txn.deltaLog, EV_ERROR, data = Map(
            "tableId" -> tableId,
            "exception" -> e.getClass.getName,
            "message" -> Option(e.getMessage).getOrElse("")
          ))
      } finally {
        InflightDMLRegistry.unregisterAcquireListener(tableId)
        asyncWorkerThread.set(false)
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        decrementInflight()
      }
    }

    private def decrementInflight(): Unit = {
      val n = inflightByTable.get(tableId)
      if (n != null) n.decrementAndGet()
    }
  }
}
