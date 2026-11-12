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

import java.io.{InterruptedIOException, UncheckedIOException}
import java.nio.channels.ClosedByInterruptException

import org.apache.spark.sql.delta.DeltaSingleCommit
import org.apache.spark.sql.delta.actions.{Action, CommitInfo}
import org.apache.spark.sql.delta.storage.{ClosableIterator, LogStore, SupportsRewinding}
import org.apache.spark.sql.delta.storage.ClosableIterator._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import io.delta.kernel.{CommitFileStatus => KernelCommitFileStatus}

/**
 * Kernel-backed [[DeltaSingleCommit]].
 *
 * @param kernelCommitFileStatus commit version and file metadata provided by Kernel
 * @param logStore log store used to read commit actions
 * @param hadoopConf Hadoop configuration used by the log store
 */
private[v2] class DeltaV2SingleCommit(
    kernelCommitFileStatus: KernelCommitFileStatus,
    logStore: LogStore,
    hadoopConf: Configuration) extends DeltaSingleCommit {

  override val version: Long = kernelCommitFileStatus.getVersion

  override lazy val timestamp: Long = getActionsIterator().processAndClose { actions =>
    actions.collectFirst { case commitInfo: CommitInfo => commitInfo }
      .flatMap(_.inCommitTimestamp)
      .getOrElse(kernelCommitFileStatus.getFileStatus.getModificationTime)
  }

  override def getActionsIterator(
      maxInMemoryFileSizeBytes: Long = 0)
      : ClosableIterator[Action] with SupportsRewinding[Action] = {
    val kernelFileStatus = kernelCommitFileStatus.getFileStatus
    val fileStatus = new FileStatus(
      kernelFileStatus.getSize, false, 1, 1L, kernelFileStatus.getModificationTime,
      new Path(kernelFileStatus.getPath))
    lazy val actions = logStore.read(fileStatus, hadoopConf).map(Action.fromJson)
    def createIterator(): ClosableIterator[Action] = {
      if (kernelFileStatus.getSize < maxInMemoryFileSizeBytes) {
        actions.iterator.toClosable
      } else {
        logStore.readAsIterator(fileStatus, hadoopConf).withClose(_.map(Action.fromJson))
      }
    }
    new ClosableIterator[Action] with SupportsRewinding[Action] {
      private var delegatedIterator = withInterruptHandling(createIterator())
      override def hasNext: Boolean = withInterruptHandling(delegatedIterator.hasNext)
      override def next(): Action = withInterruptHandling(delegatedIterator.next())
      override def close(): Unit = withInterruptHandling(delegatedIterator.close())
      override def rewind(): Unit = withInterruptHandling {
        delegatedIterator.close()
        delegatedIterator = createIterator()
      }
    }
  }

  private def withInterruptHandling[T](operation: => T): T = {
    try operation
    catch {
      case e: InterruptedIOException => throw new UncheckedIOException(e)
      case e: ClosedByInterruptException => throw new UncheckedIOException(e)
    }
  }
}

object DeltaV2SingleCommit {
  def apply(
      kernelCommitFileStatus: KernelCommitFileStatus,
      logStore: LogStore,
      hadoopConf: Configuration): DeltaV2SingleCommit =
    new DeltaV2SingleCommit(kernelCommitFileStatus, logStore, hadoopConf)
}
