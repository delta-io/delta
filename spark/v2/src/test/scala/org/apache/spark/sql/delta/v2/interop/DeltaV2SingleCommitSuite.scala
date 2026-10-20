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

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaSingleCommit
import org.apache.spark.sql.delta.actions.Action
import io.delta.spark.internal.v2.utils.KernelCommitActionsTestUtil._
import io.delta.kernel.defaults.internal.json.{JsonUtils => KernelJsonUtils}
import io.delta.kernel.internal.actions.{SingleAction => KernelSingleAction}
import io.delta.kernel.internal.data.{GenericColumnVector => KernelColumnVectorImpl}

import org.apache.spark.SparkFunSuite

class DeltaV2SingleCommitSuite extends SparkFunSuite {

  private val actionJsons = Seq(
    """{"add":{"path":"added.parquet","partitionValues":{},"size":1,
      |"modificationTime":1,"dataChange":true}}""".stripMargin,
    """{"remove":{"path":"removed.parquet","deletionTimestamp":2,"dataChange":true}}""",
    """{"metaData":{"id":"table-id","format":{"provider":"parquet","options":{}},
      |"schemaString":"{\"type\":\"struct\",\"fields\":[]}",
      |"partitionColumns":[],"configuration":{}}}""".stripMargin)
  private val expectedActions = actionJsons.map(Action.fromJson).toList
  private val rows = actionJsons.map { json =>
    KernelJsonUtils.rowFromJson(json, KernelSingleAction.FULL_SCHEMA)
  }
  private val addRow = rows(0).getStruct(KernelSingleAction.FULL_SCHEMA.indexOf("add"))
  private val removeRow = rows(1).getStruct(KernelSingleAction.FULL_SCHEMA.indexOf("remove"))
  private val metadataRow = rows(2).getStruct(KernelSingleAction.FULL_SCHEMA.indexOf("metaData"))
  private val addColumn = new KernelColumnVectorImpl(Seq(addRow, addRow).asJava, addRow.getSchema) {
    override def isNullAt(rowId: Int): Boolean = rowId != 0
  }
  private val removeColumn =
    new KernelColumnVectorImpl(Seq(removeRow, removeRow).asJava, removeRow.getSchema) {
      override def isNullAt(rowId: Int): Boolean = rowId != 1
    }
  private val metadataColumn =
    new KernelColumnVectorImpl(Seq(metadataRow).asJava, metadataRow.getSchema)
  private def kernelCommitActions: TestCommitActions = createCommitActions(
    Seq("add" -> addColumn, "remove" -> removeColumn),
    Seq(Seq("metaData" -> metadataColumn)))

  test("version and timestamp delegate to the wrapped CommitActions") {
    val kernelCommit = kernelCommitActions
    val commit: DeltaSingleCommit = DeltaV2SingleCommit(kernelCommit)
    try {
      assert(commit.version === 1L)
      assert(commit.timestamp === 0L)
    } finally {
      commit.close()
    }
    assert(kernelCommit.closed)
  }

  test("getActionsIterator reads actions across batches and supports rewind") {
    val kernelCommit = kernelCommitActions

    val commit = new DeltaV2SingleCommit(kernelCommit)
    val actions = commit.getActionsIterator()
    try {
      assert(actions.next() === expectedActions.head)

      actions.rewind()
      assert(actions.toList === expectedActions)
      assert(!actions.hasNext)
      intercept[NoSuchElementException] {
        actions.next()
      }

      assert(!kernelCommit.closed)
      commit.close()
      assert(kernelCommit.closed)
      intercept[IllegalStateException] {
        actions.rewind()
      }
    } finally {
      actions.close()
      commit.close()
    }
  }

  test("each getActionsIterator call opens an independent stream") {
    val kernelCommit = kernelCommitActions

    val commit = new DeltaV2SingleCommit(kernelCommit)
    val first = commit.getActionsIterator()
    val second = commit.getActionsIterator(maxInMemoryFileSizeBytes = 0)
    try {
      assert(first.next() === expectedActions.head)
      first.close()
      assert(!kernelCommit.closed)
      assert(second.toList === expectedActions)
    } finally {
      first.close()
      second.close()
      commit.close()
    }
  }

  test("getActionsIterator rejects nonzero memory thresholds") {
    val kernelCommit = kernelCommitActions
    val commit: DeltaSingleCommit = DeltaV2SingleCommit(kernelCommit)
    try {
      Seq(-1L, 1L, Long.MaxValue).foreach { threshold =>
        val error = intercept[AssertionError] {
          commit.getActionsIterator(maxInMemoryFileSizeBytes = threshold)
        }
        assert(error.getMessage ===
          "getActionsIterator only supports maxInMemoryFileSizeBytes = 0")
      }
      commit.getActionsIterator().processAndClose { actions =>
        assert(actions.toList === expectedActions)
      }
    } finally {
      commit.close()
    }
  }
}
