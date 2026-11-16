/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta.v2.read

import java.io.IOException
import java.nio.channels.ClosedByInterruptException
import java.util.{List => JList, Set => JSet}
import javax.annotation.Nullable

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaErrors}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.sources.{AdmittableFile, DeltaSourceOffset}
import org.apache.spark.sql.delta.storage.ClosableIterator
import org.apache.spark.sql.delta.storage.ClosableIterator.IteratorCloseOps
import com.google.common.collect.ImmutableSet
import io.delta.kernel.exceptions.{UnsupportedProtocolVersionException => KernelUnsupportedProtocolVersionException}
import io.delta.kernel.internal.DeltaLogActionUtils.{DeltaAction => KernelDeltaAction}

import org.apache.spark.sql.types.{DataType, StructType}

/** Helpers and constants of [[DeltaV2MicroBatchStream]]. */
private[read] object DeltaV2MicroBatchStreamUtils {

  val ACTION_SET: JSet[KernelDeltaAction] = ImmutableSet.builder[KernelDeltaAction]()
    .add(
      KernelDeltaAction.ADD,
      KernelDeltaAction.REMOVE,
      KernelDeltaAction.METADATA,
      KernelDeltaAction.PROTOCOL,
      KernelDeltaAction.COMMITINFO)
    .build()

  /** Action set for CDC reads: [[ACTION_SET]] plus CDC. */
  val CDC_ACTION_SET: JSet[KernelDeltaAction] =
    ImmutableSet.builder[KernelDeltaAction]().addAll(ACTION_SET).add(KernelDeltaAction.CDC).build()

  /**
   * Max attempts to resolve the start version when failOnDataLoss=false. Attempt 1 is the initial
   * try with the user's startVersion (surfaces the StartVersionNotFoundException that triggers the
   * failOnDataLoss check). Attempt 2 retries with the earliest available version to recover when
   * failOnDataLoss=false. Attempt 3 guards against a concurrent log-retention prune in the tiny
   * window between attempt 2's exception and its retry.
   */
  val FAIL_ON_DATA_LOSS_FALSE_MAX_ATTEMPTS: Int = 3

  /**
   * If the given exception wraps a [[ClosedByInterruptException]] as its direct cause, returns
   * it. This occurs when Spark interrupts the micro-batch thread during stream shutdown and the
   * thread is blocked inside Kernel's DefaultJsonHandler reading delta log files via NIO channels.
   */
  def findClosedByInterruptCause(t: Throwable): Option[ClosedByInterruptException] = {
    t.getCause match {
      case cause: ClosedByInterruptException => Some(cause)
      case _ => None
    }
  }

  /**
   * Walks the cause chain of the given throwable looking for a
   * [[KernelUnsupportedProtocolVersionException]].
   */
  def findUnsupportedProtocolCause(
      t: Throwable): Option[KernelUnsupportedProtocolVersionException] = {
    var current = t
    while (current != null) {
      current match {
        case kernelCause: KernelUnsupportedProtocolVersionException => return Some(kernelCause)
        case _ =>
      }
      if (current.getCause eq current) {
        return None
      }
      current = current.getCause
    }
    None
  }

  /**
   * Returns true if the file is strictly after the exclusive resume point (fromVersion, fromIndex).
   */
  def isAfterStartBoundary(file: IndexedFile, fromVersion: Long, fromIndex: Long): Boolean = {
    file.getVersion > fromVersion ||
      (file.getVersion == fromVersion && file.getIndex > fromIndex)
  }

  /** Result of commit validation, including skip decision and detected metadata/protocol. */
  class CommitValidationResult(
      val shouldSkipCommit: Boolean,
      @Nullable val metadataAction: Metadata,
      @Nullable val protocolAction: Protocol)

  /**
   * Validates the partition schema and physical names without comparing unrelated metadata.
   *
   * Partition changes need a separate check because full-schema validation sorts the columns and
   * does not distinguish partition columns from data columns.
   */
  def validatePartitionSchemaOnStartup(
      partitionSchema: StructType,
      readPartitionSchemaAtSourceInit: StructType,
      tableId: String): Unit = {
    // Physical names identify the partition values in each file.
    // Partition reads tolerate reordering, but keep this check consistent with the order-sensitive
    // partition-change checks.
    val namesMatch =
      partitionSchema.fieldNames.sameElements(readPartitionSchemaAtSourceInit.fieldNames)
    val typesMatch = DataType.equalsStructurally(
      partitionSchema, readPartitionSchemaAtSourceInit, ignoreNullability = false)
    val physicalNamesMatch = partitionSchema.fields.map(DeltaColumnMapping.getPhysicalName)
      .sameElements(readPartitionSchemaAtSourceInit.fields.map(DeltaColumnMapping.getPhysicalName))
    if (!namesMatch || !typesMatch || !physicalNamesMatch) {
      throw DeltaErrors.streamingPartitionSchemaMismatchOnRestart(
        tableId, partitionSchema, readPartitionSchemaAtSourceInit)
    }
  }

  /**
   * Validates that the analysis-time schema matches the latest snapshot schema. This catches cases
   * where the table schema changed after the streaming query was analyzed, and the user restarts
   * the stream with a stale DataFrame.
   *
   * @param dataSchema data columns from analysis time (excludes partition columns)
   * @param partitionSchema partition columns from analysis time
   * @param snapshotSchema full table schema from the latest snapshot at stream start
   */
  def validateSchemaCompatibilityOnStartup(
      dataSchema: StructType,
      partitionSchema: StructType,
      snapshotSchema: StructType): Unit = {
    // Reconstruct the full analysis-time table schema without modifying the original schemas.
    val querySchema = StructType(dataSchema.fields ++ partitionSchema.fields)
    // Sort both sides so a partition column declared in the middle of the table doesn't
    // trip the check.
    val sortedQuery = sortFieldsByName(querySchema)
    val sortedSnapshot = sortFieldsByName(snapshotSchema)
    // equalsStructurally checks types + nullability but ignores field names;
    // equalsStructurallyByName checks names but ignores types/nullability.
    val typesAndNullabilityMatch =
      DataType.equalsStructurally(sortedQuery, sortedSnapshot, ignoreNullability = false)
    val namesMatch = DataType.equalsStructurallyByName(
      sortedQuery, sortedSnapshot, (a, b) => a.equalsIgnoreCase(b))
    if (!typesAndNullabilityMatch || !namesMatch) {
      throw DeltaErrors.streamingSchemaMismatchOnRestart(querySchema, snapshotSchema)
    }
  }

  private def sortFieldsByName(schema: StructType): StructType = {
    StructType(schema.fields.sortBy(_.name))
  }

  /** Closes the current commit's files before the owning commit iterator. */
  def wrapIteratorWithCommitIteratorClose[T](
      inner: ClosableIterator[T],
      commits: ClosableIterator[_]): ClosableIterator[T] = {
    new ClosableIterator[T] {
      private var innerClosed = false

      override def hasNext: Boolean = inner.hasNext

      override def next(): T = inner.next()

      @throws[IOException]
      override def close(): Unit = {
        var failure: Throwable = null
        try {
          if (!innerClosed) {
            innerClosed = true
            inner.close()
          }
        } catch {
          // Preserve the inner failure even when cleanup fails with a fatal exception.
          case t: Throwable =>
            failure = t
            throw t
        } finally {
          if (commits != null) {
            if (failure == null) {
              commits.close()
            } else {
              try {
                commits.close()
              } catch {
                case closeFailure: Throwable => failure.addSuppressed(closeFailure)
              }
            }
          }
        }
      }
    }
  }

  /**
   * Adds dummy BEGIN_INDEX and END_INDEX IndexedFiles for `version` before and after the contents
   * of the iterator. The contents must be the IndexedFiles that correspond to this version.
   */
  def addBeginAndEndIndexOffsetsForVersion(
      version: Long,
      files: ClosableIterator[IndexedFile]): ClosableIterator[IndexedFile] = {
    files.withClose { iterator =>
      Iterator.single(IndexedFile.sentinel(version, DeltaSourceOffset.BASE_INDEX)) ++
        iterator ++ Iterator.single(IndexedFile.sentinel(version, DeltaSourceOffset.END_INDEX))
    }
  }

  /** Converts a list of IndexedFiles to a Scala Seq of AdmittableFile for batch admission. */
  def toScalaAdmittableSeq(files: JList[IndexedFile]): Seq[AdmittableFile] = {
    files.asScala.toList
  }

  /** Checks if the endOffset is at BASE_INDEX for the given version (early exit condition). */
  def isEndOffsetAtBaseIndex(endOffsetOpt: Option[DeltaSourceOffset], version: Long): Boolean = {
    endOffsetOpt.exists { endOffset =>
      endOffset.reservoirVersion == version && endOffset.index == DeltaSourceOffset.BASE_INDEX
    }
  }
}
