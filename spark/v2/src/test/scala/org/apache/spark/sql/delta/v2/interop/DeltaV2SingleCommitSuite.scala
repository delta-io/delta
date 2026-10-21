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
import io.delta.kernel.data.{ColumnarBatch => KernelColumnarBatch}
import io.delta.kernel.defaults.internal.json.{JsonUtils => KernelJsonUtils}
import io.delta.kernel.internal.actions.{SingleAction => KernelSingleAction}
import io.delta.kernel.internal.data.{GenericColumnVector => KernelColumnVectorImpl}
import io.delta.kernel.utils.{CloseableIterator => KernelCloseableIterator}

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
  private def kernelCommitActions: TestCommitActions = {
    val kernelBatchColumns = actionJsons.zip(Seq("add", "remove", "metaData")).map {
      case (json, actionName) =>
        val kernelRow = KernelJsonUtils.rowFromJson(json, KernelSingleAction.FULL_SCHEMA)
          .getStruct(KernelSingleAction.FULL_SCHEMA.indexOf(actionName))
        Seq(actionName -> new KernelColumnVectorImpl(Seq(kernelRow).asJava, kernelRow.getSchema))
    }
    createCommitActions(kernelBatchColumns.head, kernelBatchColumns.tail)
  }

  test("getActionsIterator supports rewind after partial consumption and exhaustion") {
    val kernelCommit = kernelCommitActions
    val commit = new DeltaV2SingleCommit(kernelCommit)
    assert(kernelCommit.closed)
    assert(commit.version === 1L)
    assert(commit.timestamp === 0L)
    val actions = commit.getActionsIterator()
    try {
      assert(actions.next() === expectedActions.head)
      actions.rewind()
      assert(actions.toList === expectedActions)
      assert(!actions.hasNext)
      intercept[NoSuchElementException] {
        actions.next()
      }
      actions.rewind()
      assert(actions.toList === expectedActions)
    } finally {
      actions.close()
    }
  }

  test("each getActionsIterator call opens an independent stream") {
    val kernelCommit = kernelCommitActions
    val commit = new DeltaV2SingleCommit(kernelCommit)
    assert(kernelCommit.closed)
    val first = commit.getActionsIterator()
    val second = commit.getActionsIterator(maxInMemoryFileSizeBytes = 0)
    try {
      assert(first.next() === expectedActions.head)
      first.close()
      assert(second.toList === expectedActions)
      first.rewind()
      assert(first.toList === expectedActions)
    } finally {
      first.close()
      second.close()
    }
  }

  test("getActionsIterator rejects nonzero memory thresholds") {
    val commit: DeltaSingleCommit = DeltaV2SingleCommit(kernelCommitActions)
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
  }

  test("construction closes the Kernel commit when materialization fails") {
    val failure = new IllegalStateException("Cannot read action batches")
    val kernelCommit = new TestCommitActions(Seq.empty) {
      override def getActions(): KernelCloseableIterator[KernelColumnarBatch] = throw failure
    }
    val error = intercept[IllegalStateException] {
      DeltaV2SingleCommit(kernelCommit)
    }
    assert(error eq failure)
    assert(kernelCommit.closed)
  }
}
