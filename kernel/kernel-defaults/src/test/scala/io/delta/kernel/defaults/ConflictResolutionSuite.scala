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
package io.delta.kernel.defaults

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.{TestUtilsWithTableManagerAPIs, WriteUtils}
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.ConcurrentWriteException
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.actions.{AddFile, DeletionVectorDescriptor, RemoveFile, SingleAction}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.util.PartitionUtils
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.utils.CloseableIterable
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for Kernel's delete-vs-delete conflict detection (see `ConflictChecker`).
 *
 * This is a standalone suite (not part of the `AbstractDeltaTableWritesSuite` hierarchy) on
 * purpose: these tests perform conflict-rebased commits, which do not produce a post-commit
 * snapshot, so the post-commit-snapshot CRC write variants (which subclass
 * `DeltaTableWritesSuite`) cannot run them. Conflict resolution is engine/commit-path logic, so a
 * single suite suffices.
 */
class ConflictResolutionSuite
    extends AnyFunSuite
    with WriteUtils
    with TestUtilsWithTableManagerAPIs {

  private def iterableOf(rows: Row*): CloseableIterable[Row] =
    inMemoryIterable(toCloseableIterator(rows.toSeq.asJava.iterator()))

  private def createAddFileRow(path: String, dataChange: Boolean = true): Row = {
    val partitionValues =
      PartitionUtils.serializePartitionMap(Collections.emptyMap[String, Literal]())
    val addFileRow = AddFile.createAddFileRow(
      testSchema,
      path,
      partitionValues,
      100L, // size
      System.currentTimeMillis(), // modificationTime
      dataChange,
      Optional.empty(), // deletionVector
      Optional.empty(), // tags
      Optional.empty(), // baseRowId
      Optional.empty(), // defaultRowCommitVersion
      Optional.empty() // stats
    )
    SingleAction.createAddFileSingleAction(addFileRow)
  }

  private def createRemoveFileRow(path: String, dataChange: Boolean = true): Row = {
    val removeFileRow = new GenericRow(
      RemoveFile.FULL_SCHEMA,
      Map[Integer, Object](
        Integer.valueOf(RemoveFile.FULL_SCHEMA.indexOf("path")) -> path,
        Integer.valueOf(RemoveFile.FULL_SCHEMA.indexOf("deletionTimestamp")) -> Long.box(
          System.currentTimeMillis()),
        Integer.valueOf(RemoveFile.FULL_SCHEMA.indexOf("dataChange")) -> Boolean.box(dataChange),
        Integer.valueOf(RemoveFile.FULL_SCHEMA.indexOf("size")) -> Long.box(100L)).asJava)
    SingleAction.createRemoveFileSingleAction(removeFileRow)
  }

  private def pathDv(uniqueSuffix: String): DeletionVectorDescriptor =
    new DeletionVectorDescriptor(
      "p", // path storage
      s"/deletion_vector_$uniqueSuffix.bin",
      Optional.of(Integer.valueOf(0)),
      32, // sizeInBytes
      1L
    ) // cardinality

  /** An AddFile that re-adds `path` carrying the given deletion vector (a merge-on-read update). */
  private def createAddFileRowWithDv(path: String, dv: DeletionVectorDescriptor): Row = {
    val partitionValues =
      PartitionUtils.serializePartitionMap(Collections.emptyMap[String, Literal]())
    val addFileRow = AddFile.createAddFileRow(
      testSchema,
      path,
      partitionValues,
      100L, // size
      System.currentTimeMillis(), // modificationTime
      true, // dataChange
      Optional.of(dv), // deletionVector
      Optional.empty(), // tags
      Optional.empty(), // baseRowId
      Optional.empty(), // defaultRowCommitVersion
      Optional.empty() // stats
    )
    SingleAction.createAddFileSingleAction(addFileRow)
  }

  /** Create a table and seed it with two data files (file1.parquet, file2.parquet). */
  private def createTableWithTwoFiles(engine: Engine, tablePath: String): Unit = {
    commitTransaction(getCreateTxn(engine, tablePath, testSchema), engine, emptyIterable())
    commitTransaction(
      getUpdateTxn(engine, tablePath),
      engine,
      iterableOf(
        createAddFileRow("file1.parquet"),
        createAddFileRow("file2.parquet")))
  }

  test("conflict resolution - two concurrent txns removing the same file fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithTwoFiles(engine, tablePath)

      // Both transactions read the same starting version.
      val losingTxn = getUpdateTxn(engine, tablePath)
      val winningTxn = getUpdateTxn(engine, tablePath)

      // Winner rewrites file1.parquet (remove + add), committing first.
      commitTransaction(
        winningTxn,
        engine,
        iterableOf(
          createRemoveFileRow("file1.parquet"),
          createAddFileRow("winner-new.parquet")))

      // Loser also removes file1.parquet -> delete-vs-delete conflict.
      val e = intercept[ConcurrentWriteException] {
        commitTransaction(
          losingTxn,
          engine,
          iterableOf(
            createRemoveFileRow("file1.parquet"),
            createAddFileRow("loser-new.parquet")))
      }
      assert(e.getMessage.contains("file1.parquet"))
    }
  }

  test("conflict resolution - two concurrent DV updates to the same file fail") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithTwoFiles(engine, tablePath)

      val losingTxn = getUpdateTxn(engine, tablePath)
      val winningTxn = getUpdateTxn(engine, tablePath)

      // Winner attaches a DV to file1.parquet: remove the old entry, re-add it with the new DV.
      commitTransaction(
        winningTxn,
        engine,
        iterableOf(
          createRemoveFileRow("file1.parquet"),
          createAddFileRowWithDv("file1.parquet", pathDv("winner"))))

      // Loser attaches a different DV to the same file -> delete-vs-delete conflict.
      val e = intercept[ConcurrentWriteException] {
        commitTransaction(
          losingTxn,
          engine,
          iterableOf(
            createRemoveFileRow("file1.parquet"),
            createAddFileRowWithDv("file1.parquet", pathDv("loser"))))
      }
      assert(e.getMessage.contains("file1.parquet"))
    }
  }

  test("conflict resolution - concurrent txns removing different files succeed") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithTwoFiles(engine, tablePath)

      val losingTxn = getUpdateTxn(engine, tablePath)
      val winningTxn = getUpdateTxn(engine, tablePath)

      commitTransaction(winningTxn, engine, iterableOf(createRemoveFileRow("file1.parquet")))

      // Removing a different file is not a conflict; the losing txn rebases and commits.
      commitTransaction(losingTxn, engine, iterableOf(createRemoveFileRow("file2.parquet")))
    }
  }

  test("conflict resolution - appending while a concurrent txn removes a file succeeds") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithTwoFiles(engine, tablePath)

      val losingTxn = getUpdateTxn(engine, tablePath)
      val winningTxn = getUpdateTxn(engine, tablePath)

      commitTransaction(winningTxn, engine, iterableOf(createRemoveFileRow("file1.parquet")))

      // The losing txn removes nothing, so the delete-vs-delete check must not fire.
      commitTransaction(losingTxn, engine, iterableOf(createAddFileRow("file3.parquet")))
    }
  }
}
