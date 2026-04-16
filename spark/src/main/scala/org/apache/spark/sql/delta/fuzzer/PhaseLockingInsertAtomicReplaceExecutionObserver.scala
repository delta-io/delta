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

package org.apache.spark.sql.delta.fuzzer

import scala.concurrent.duration._

import org.apache.spark.sql.delta.{GetChainableExecutionObserver, InsertAtomicReplaceExecutionObserver}

/**
 * An execution observer for INSERT REPLACE WHERE/ON/USING that allows enforcing when each job
 * gets executed.
 * Use [[InsertAtomicReplaceExecutionController]] to control execution from tests.
 */
private[delta] case class PhaseLockingInsertAtomicReplaceExecutionObserver(
    phases: InsertAtomicReplacePhases)
  extends InsertAtomicReplaceExecutionObserver
  with PhaseLockingExecutionObserver
  with GetChainableExecutionObserver[InsertAtomicReplaceExecutionObserver] {

  override def getNextObserver: Option[PhaseLockingInsertAtomicReplaceExecutionObserver] =
    super.getNextObserver.map(_.asInstanceOf[PhaseLockingInsertAtomicReplaceExecutionObserver])

  val phaseLocks: Seq[ExecutionPhaseLock] = Seq(
    phases.delete,
    phases.insert,
    phases.commit)

  override def delete[T](f: => T): T = phases.delete.execute(f)

  override def insert[T](f: => T): T = phases.insert.execute(f)

  override def commit[T](f: => T): T = phases.commit.execute(f)

}

/**
 * Controls the order of execution of the two INSERT REPLACE WHERE/ON/USING jobs
 * via the provided observer.
 */
class InsertAtomicReplaceExecutionController(
    private var currentObserver: PhaseLockingInsertAtomicReplaceExecutionObserver)
  extends ExecutionController {

  override def observer: PhaseLockingInsertAtomicReplaceExecutionObserver = currentObserver

  /**
   * Time to wait before throwing an exception when the transaction execution is not making
   * progress, usually because of a deadlock.
   */
  private val insertAtomicReplaceTimeout: FiniteDuration = 10.minutes

  private def phases: InsertAtomicReplacePhases = observer.phases

  /**
   * Set the thread-local variable that instruments INSERT REPLACE WHERE/ON/USING.
   */
  override def withObserver[T](fn: => T): T =
    InsertAtomicReplaceExecutionObserver.withObserver(observer)(fn)

  /**
   * Unblocks the `delete` phase and waits for it to complete.
   */
  def runDelete(): Unit = {
    checkPhaseCanStart(phase = phases.delete)
    phases.delete.entryBarrier.unblock()
    busyWaitFor(phases.delete.hasLeft, insertAtomicReplaceTimeout)
  }

  /**
   * Unblocks the `insert` phase and waits for it to complete.
   */
  def runInsert(): Unit = {
    checkPhaseCanStart(phase = phases.insert)
    phases.insert.entryBarrier.unblock()
    busyWaitFor(phases.insert.hasLeft, insertAtomicReplaceTimeout)
  }

  /**
   * Unblocks all phases in the entire observer chain and waits for the
   * INSERT REPLACE WHERE/ON/USING operation to complete.
   */
  def runToCompletion(): Unit = {
    unblockAllPhases()
    busyWaitFor(observer.phases.commit.hasLeft, insertAtomicReplaceTimeout)
    assert(observer.getNextObserver.isEmpty)
  }
}

case class InsertAtomicReplacePhases(
    delete: ExecutionPhaseLock,
    insert: ExecutionPhaseLock,
    commit: ExecutionPhaseLock)

object InsertAtomicReplacePhases {

  private final val PREFIX = "INSERT_ATOMIC_REPLACE_"

  private final val DELETE_PHASE_LABEL = PREFIX + "DELETE"
  private final val INSERT_PHASE_LABEL = PREFIX + "INSERT"
  private final val COMMIT_PHASE_LABEL = PREFIX + "COMMIT"

  def apply(): InsertAtomicReplacePhases = {
    InsertAtomicReplacePhases(
      delete = ExecutionPhaseLock(DELETE_PHASE_LABEL),
      insert = ExecutionPhaseLock(INSERT_PHASE_LABEL),
      commit = ExecutionPhaseLock(COMMIT_PHASE_LABEL))
  }
}
