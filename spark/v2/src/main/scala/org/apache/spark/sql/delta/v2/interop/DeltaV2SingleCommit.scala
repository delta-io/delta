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
package org.apache.spark.sql.delta.v2.interop

import org.apache.spark.sql.delta.DeltaSingleCommit
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.storage.{ClosableIterator, SupportsRewinding}
import org.apache.spark.sql.delta.v2.kernel.KernelActionUtils
import io.delta.kernel.{CommitActions => KernelCommitActions}
import io.delta.kernel.internal.util.Utils

/**
 * Kernel-backed [[DeltaSingleCommit]].
 *
 * @param singleKernelCommit the single Kernel commit.
 */
private[interop] class DeltaV2SingleCommit(
    private val singleKernelCommit: KernelCommitActions
) extends DeltaSingleCommit {

  private val (commitVersion, commitTimestamp, actions): (Long, Long, Seq[Action]) = try {
    val version = singleKernelCommit.getVersion()
    val timestamp = singleKernelCommit.getTimestamp()
    val actions = KernelActionUtils.readActions(singleKernelCommit).processAndClose(_.toVector)
    (version, timestamp, actions)
  } finally {
    Utils.closeCloseables(singleKernelCommit)
  }

  override val version: Long = commitVersion

  override val timestamp: Long = commitTimestamp

  override def getActionsIterator(
      maxInMemoryFileSizeBytes: Long): ClosableIterator[Action] with SupportsRewinding[Action] = {
    // Actions are already materialized, so the file-size threshold does not affect rewinds.
    new ClosableIterator[Action] with SupportsRewinding[Action] {
      private var delegatedIterator = actions.iterator
      override def hasNext: Boolean = delegatedIterator.hasNext
      override def next(): Action = delegatedIterator.next()
      override def close(): Unit = ()
      override def rewind(): Unit = delegatedIterator = actions.iterator
    }
  }
}

object DeltaV2SingleCommit {
  def apply(singleKernelCommit: KernelCommitActions): DeltaSingleCommit =
    new DeltaV2SingleCommit(singleKernelCommit)
}
