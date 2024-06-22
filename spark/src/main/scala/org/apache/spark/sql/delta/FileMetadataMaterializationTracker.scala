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

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.delta.FileMetadataMaterializationTracker.TaskLevelPermitAllocator
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.internal.{LoggingShims, MDC}

/**
 * An instance of this class tracks and controls the materialization usage of a single command
 * query (e.g. Backfill) with respect to the driver limits. Each query must use one instance of the
 * FileMaterializationTracker.
 *
 * tasks - tasks are the basic unit of computation.
 * For example, in Backfill, each task bins multiple files into batches to be executed.
 *
 * A task has to be materialized in its entirety, so in the case where we are unable to acquire
 * permits to materialize a task we acquire an over allocation lock that will allow tasks to
 * complete materializing. Over allocation is only allowed for one thread at once in the driver.
 * This allows us to restrict the amount of file metadata being materialized at once on the driver.
 *
 * Accessed by the thread materializing files and by the thread releasing resources after execution.
 *
 */
class FileMetadataMaterializationTracker extends LoggingShims {

  /** The number of permits allocated from the global file materialization semaphore */
  @volatile private var numPermitsFromSemaphore: Int = 0

  /** The number of permits over allocated by holding the overAllocationLock */
  @volatile private var numOverAllocatedPermits: Int = 0

  private val materializationMetrics = new FileMetadataMaterializationMetrics()

  /**
   * A per task permit allocator which allows materializing a new task.
   * @return - TaskLevelPermitAllocator to be used to materialize a task
   */
  def createTaskLevelPermitAllocator(): TaskLevelPermitAllocator = {
    new TaskLevelPermitAllocator(this)
  }

  /**
   * Acquire a permit from the materialization semaphore, if there is no permit available the thread
   * acquires the overAllocationLock which allows it to freely acquire permits in the future.
   * Only one thread can over allocate at once.
   *
   * @param isNewTask - indicates whether the permit is being acquired for a new task, this will
   *                 allow us to prevent overallocation to spill over to new tasks.
   */
  private def acquirePermit(isNewTask: Boolean = false): Unit = {
    var hasAcquiredPermit = false
    if (isNewTask) {
      FileMetadataMaterializationTracker.materializationSemaphore.acquire(1)
      hasAcquiredPermit = true
    } else if (numOverAllocatedPermits > 0) {
      materializationMetrics.overAllocFilesMaterializedCount += 1
    } else if (!FileMetadataMaterializationTracker.materializationSemaphore.tryAcquire(1)) {
      // we acquire the overAllocationLock for this thread
      logInfo("Acquiring over allocation lock for this query.")
      val startTime = System.currentTimeMillis()
      FileMetadataMaterializationTracker.overAllocationLock.acquire(1)
      val waitTime = System.currentTimeMillis() - startTime
      logInfo(log"Acquired over allocation lock for this query in " +
        log"${MDC(DeltaLogKeys.TIME_MS, waitTime)} ms")
      materializationMetrics.overAllocWaitTimeMs += waitTime
      materializationMetrics.overAllocWaitCount += 1
      materializationMetrics.overAllocFilesMaterializedCount += 1
    } else {
      // tryAcquire was successful
      hasAcquiredPermit = true
    }
    if (hasAcquiredPermit) {
      this.synchronized {
        numPermitsFromSemaphore += 1
      }
    } else {
      this.synchronized {
        numOverAllocatedPermits += 1
      }
    }
    materializeOneFile()
  }

  /** Increment the number of materialized file in materializationMetrics. */
  def materializeOneFile(): Unit = materializationMetrics.filesMaterializedCount += 1

  /**
   * Release `numPermits` file permits and release overAllocationLock lock if held by the thread
   * and the number of over allocated files is 0.
   */
  def releasePermits(numPermits: Int): Unit = {
    var permitsToRelease = numPermits
    this.synchronized {
      if (numOverAllocatedPermits > 0) {
        val overAllocatedPermitsToRelease = Math.min(numOverAllocatedPermits, numPermits)
        numOverAllocatedPermits -= overAllocatedPermitsToRelease
        permitsToRelease -= overAllocatedPermitsToRelease
        if (numOverAllocatedPermits == 0) {
          FileMetadataMaterializationTracker.overAllocationLock.release(1)
          logInfo("Released over allocation lock.")
        }
      }
      numPermitsFromSemaphore -= permitsToRelease
    }
    FileMetadataMaterializationTracker.materializationSemaphore.release(permitsToRelease)
  }

  /**
   * This will release all acquired file permits by the tracker.
   */
  def releaseAllPermits(): Unit = {
    this.synchronized {
      if (numOverAllocatedPermits > 0) {
        FileMetadataMaterializationTracker.overAllocationLock.release(1)
      }
      if (numPermitsFromSemaphore > 0) {
        FileMetadataMaterializationTracker.materializationSemaphore.release(numPermitsFromSemaphore)
      }
      numPermitsFromSemaphore = 0
      numOverAllocatedPermits = 0
    }
  }
}

object FileMetadataMaterializationTracker extends DeltaLogging {
  // Global limit for number of files that can be materialized at once on the driver
  private val globalFileMaterializationLimit: AtomicInteger = new AtomicInteger(-1)

  // Semaphore to control file materialization
  private var materializationSemaphore: Semaphore = _

  /**
   * Global lock that is held by a thread and allows it to materialize files without
   * acquiring permits the materializationSemaphore.
   *
   * This lock is released when the thread completes executing the command's job that
   * acquired it, or when all permits are released during bin packing.
   */
  private val overAllocationLock = new Semaphore(1)

  /**
   * Initialize the global materialization semaphore using an existing semaphore. This is used
   * for unit tests.
   */
  private[sql] def initializeSemaphoreForTests(semaphore: Semaphore): Unit = {
    globalFileMaterializationLimit.set(semaphore.availablePermits())
    materializationSemaphore = semaphore
  }

  /**
   * A per task level allocator that controls permit allocation and releasing for the task
   */
  class TaskLevelPermitAllocator(tracker: FileMetadataMaterializationTracker) {

    /** Indicates whether the file materialization is for a new task */
    var isNewTask = true

    /**
     * Acquire a single file materialization permit.
     */
    def acquirePermit(): Unit = {
      if (isNewTask) {
        logInfo("Acquiring file materialization permits for a new task")
      }
      tracker.acquirePermit(isNewTask = isNewTask)
      isNewTask = false
    }
  }
}

/**
 * Instance of this class is used for recording metrics of the FileMetadataMaterializationTracker
 */
case class FileMetadataMaterializationMetrics(
  /** Total number of files materialized */
  var filesMaterializedCount: Long = 0L,

  /** Number of times we wait to acquire the over allocation lock */
  var overAllocWaitCount: Long = 0L,

  /** Total time waited to acquire the over allocation lock in ms */
  var overAllocWaitTimeMs: Long = 0L,

  /** Number of files materialized by using over allocation lock */
  var overAllocFilesMaterializedCount: Long = 0L) {

  override def toString(): String = {
    s"Number of files materialized: $filesMaterializedCount, " +
      s"Number of times over-allocated: $overAllocWaitCount, " +
      s"Total time spent waiting to acquire over-allocation lock: $overAllocWaitTimeMs, " +
      s"Files materialized by over allocation: $overAllocFilesMaterializedCount"
  }
}
