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

case class OptimisticTransactionPhases(
    initialPhase: ExecutionPhaseLock,
    preparePhase: ExecutionPhaseLock,
    commitPhase: ExecutionPhaseLock,
    backfillPhase: ExecutionPhaseLock)

object OptimisticTransactionPhases {

  private final val PREFIX = "TXN_"

  final val INITIAL_PHASE_LABEL = PREFIX + "INIT"
  final val PREPARE_PHASE_LABEL = PREFIX + "PREPARE"
  final val COMMIT_PHASE_LABEL = PREFIX + "COMMIT"
  final val BACKFILL_PHASE_LABEL = PREFIX + "BACKFILL"

  def forName(txnName: String): OptimisticTransactionPhases = {

    def toTxnPhaseLabel(phaseLabel: String): String =
      txnName + "-" + phaseLabel

    OptimisticTransactionPhases(
      initialPhase = ExecutionPhaseLock(toTxnPhaseLabel(INITIAL_PHASE_LABEL)),
      preparePhase = ExecutionPhaseLock(toTxnPhaseLabel(PREPARE_PHASE_LABEL)),
      commitPhase = ExecutionPhaseLock(toTxnPhaseLabel(COMMIT_PHASE_LABEL)),
      backfillPhase = ExecutionPhaseLock(toTxnPhaseLabel(BACKFILL_PHASE_LABEL)))
  }
}
