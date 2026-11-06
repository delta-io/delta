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

import java.io.Closeable

import org.apache.spark.sql.delta.DeltaSingleCommit
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.storage.{ClosableIterator, SupportsRewinding}
import org.apache.spark.sql.delta.v2.kernel.KernelActionUtils
import io.delta.kernel.{CommitActions => KernelCommitActions}
import io.delta.kernel.internal.util.{Utils => KernelUtils}

// TODO: Once interop supports commit caching, restore private[interop]
// and remove with Closeable.
/**
 * Kernel-backed [[DeltaSingleCommit]].
 *
 * @param singleKernelCommit the single Kernel commit.
 */
private[v2] class DeltaV2SingleCommit(
    private val singleKernelCommit: KernelCommitActions
) extends DeltaSingleCommit with Closeable {

  private val (commitVersion, commitTimestamp): (Long, Long) = try {
    (singleKernelCommit.getVersion(), singleKernelCommit.getTimestamp())
  } catch {
    case error: Throwable =>
      KernelUtils.closeCloseables(singleKernelCommit)
      throw error
  }

  override val version: Long = commitVersion

  override val timestamp: Long = commitTimestamp

  override def getActionsIterator(
      maxInMemoryFileSizeBytes: Long): ClosableIterator[Action] with SupportsRewinding[Action] = {
    // Kernel does not expose the commit's file size, so always use the zero-cache path.
    new ClosableIterator[Action] with SupportsRewinding[Action] {
      private var delegatedIterator =
        KernelActionUtils.readActions(singleKernelCommit)
      override def hasNext: Boolean =
        delegatedIterator.hasNext
      override def next(): Action =
        delegatedIterator.next()
      override def close(): Unit = delegatedIterator.close()
      override def rewind(): Unit = {
        close()
        delegatedIterator =
          KernelActionUtils.readActions(singleKernelCommit)
      }
    }
  }

  override def close(): Unit = KernelUtils.closeCloseables(singleKernelCommit)
}

object DeltaV2SingleCommit {
  def apply(singleKernelCommit: KernelCommitActions): DeltaSingleCommit =
    new DeltaV2SingleCommit(singleKernelCommit)
}
