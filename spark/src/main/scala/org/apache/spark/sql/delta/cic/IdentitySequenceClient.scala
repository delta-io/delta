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

// Lives in an `org.apache.spark.*` package because the RpcEnv / RpcEndpoint types it uses are
// `private[spark]`.
package org.apache.spark.sql.delta.cic

import org.apache.spark.SparkEnv
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.util.RpcUtils

// EXECUTOR SIDE: runs inside a write task, reaching the driver endpoint over RpcEnv to reserve
// ranges mid-write. Holds no reservation logic; it reports its previous reserve and the driver
// sizes the next.

/** Executor-side client of the driver's [[IdentitySequenceCoordinator]] endpoint. */
object IdentitySequenceClient {

  // Memoized per RpcEnv and has to be re-resolved if the RpcEnv changes.
  private var cachedRef: (RpcEnv, RpcEndpointRef) = _
  private def driverRef: RpcEndpointRef = synchronized {
    val env = SparkEnv.get
    if (cachedRef == null || (cachedRef._1 ne env.rpcEnv)) {
      cachedRef = (env.rpcEnv,
        RpcUtils.makeDriverRef(IdentitySequenceCoordinator.ENDPOINT_NAME, env.conf, env.rpcEnv))
    }
    cachedRef._2
  }

  /**
   * Blocking reserve, reporting the caller's PREVIOUS reserve `(lastCount, elapsedMs)` for the
   * driver's sizing strategy (`0`/`0` first reserve -> initial size). The reply may be shorter than
   * asked (generator reserves again on exhaustion). No retry: a failed `askSync` aborts the task,
   * so no out-of-range value is committed.
   */
  def requestIds(
      sequenceId: String,
      tableId: String,
      lastCount: Long,
      elapsedMs: Long,
      step: Long,
      stageId: Int,
      partitionId: Int,
      taskAttemptId: Long): IdRange = {
    // Ships the serializable reserve request over RpcEnv; Spark serializes the RPC message
    // itself. See [[IdentitySequenceCoordinatorEndpoint.receiveAndReply]].
    driverRef.askSync[IdRange](
      RequestIds(tableId, sequenceId, lastCount, elapsedMs, step, stageId, partitionId,
        taskAttemptId))
  }
}
