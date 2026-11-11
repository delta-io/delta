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

import java.io.IOException

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaSingleCommit
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.v2.utils.KernelCommitActionsTestUtil._
import org.mockito.Mockito.{doReturn, doThrow, inOrder, mock, spy, verify, when}
import io.delta.kernel.{CommitActions => KernelCommitActions}
import io.delta.kernel.data.{ColumnarBatch => KernelColumnarBatch}
import io.delta.kernel.defaults.internal.json.{JsonUtils => KernelJsonUtils}
import io.delta.kernel.internal.actions.{SingleAction => KernelSingleAction}
import io.delta.kernel.internal.data.{GenericColumnVector => KernelColumnVectorImpl}
import io.delta.kernel.utils.{CloseableIterator => KernelCloseableIterator}

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

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
    assert(!kernelCommit.closed)
    assert(kernelCommit.actionBatchesRead === 0)
    assert(commit.version === 1L)
    assert(commit.timestamp === 0L)
    val actions = commit.getActionsIterator()
    try {
      assert(actions.next() === expectedActions.head)
      assert(kernelCommit.actionBatchesRead === 1)
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
      commit.close()
    }
    assert(kernelCommit.closed)
  }

  test("each getActionsIterator call opens an independent stream") {
    val kernelCommit = kernelCommitActions
    val commit = new DeltaV2SingleCommit(kernelCommit)
    assert(!kernelCommit.closed)
    val first = commit.getActionsIterator()
    val second = commit.getActionsIterator(maxInMemoryFileSizeBytes = 0)
    try {
      assert(first.next() === expectedActions.head)
      first.close()
      assert(!kernelCommit.closed)
      assert(second.toList === expectedActions)
      first.rewind()
      assert(first.toList === expectedActions)
    } finally {
      first.close()
      second.close()
      commit.close()
    }
    assert(kernelCommit.closed)
  }

  Seq(-1L, 0L, 1L, Long.MaxValue).foreach { threshold =>
    test(s"getActionsIterator streams regardless of the memory threshold ($threshold)") {
      val kernelCommit = kernelCommitActions
      val commit: DeltaSingleCommit = DeltaV2SingleCommit(kernelCommit)
      val actions = commit.getActionsIterator(maxInMemoryFileSizeBytes = threshold)
      try {
        assert(kernelCommit.actionBatchesRead === 0)
        assert(actions.next() === expectedActions.head)
        assert(kernelCommit.actionBatchesRead === 1)
        actions.rewind()
        assert(actions.toList === expectedActions)
      } finally {
        actions.close()
        commit.asInstanceOf[DeltaV2SingleCommit].close()
      }
      assert(kernelCommit.closed)
    }
  }

  // Mocks let these tests inject resource failures and verify close order.
  test("action iterators close before closing each Kernel commit") {
    val kernelFirstCommit = mock(classOf[KernelCommitActions])
    val kernelSecondCommit = mock(classOf[KernelCommitActions])
    val kernelFirstActions = mock(classOf[KernelCloseableIterator[KernelColumnarBatch]])
    val kernelSecondActions = mock(classOf[KernelCloseableIterator[KernelColumnarBatch]])
    when(kernelFirstCommit.getActions()).thenReturn(kernelFirstActions)
    when(kernelSecondCommit.getActions()).thenReturn(kernelSecondActions)

    Seq(kernelFirstCommit, kernelSecondCommit).foreach { kernelCommit =>
      val commit = new DeltaV2SingleCommit(kernelCommit)
      try {
        commit.getActionsIterator().processAndClose { actions =>
          assert(actions.isEmpty)
        }
      } finally {
        commit.close()
      }
    }

    val order = inOrder(
      kernelFirstActions, kernelFirstCommit, kernelSecondActions, kernelSecondCommit)
    order.verify(kernelFirstActions).close()
    order.verify(kernelFirstCommit).close()
    order.verify(kernelSecondCommit).getActions()
    order.verify(kernelSecondActions).close()
    order.verify(kernelSecondCommit).close()
  }

  test("opening actions propagates IO failures and leaves the commit for caller cleanup") {
    val failure = new IOException("action iterator open failed")
    val kernelCommit = mock(classOf[KernelCommitActions])
    when(kernelCommit.getVersion()).thenReturn(7L)
    when(kernelCommit.getActions()).thenAnswer { _ => throw failure }

    val commit = new DeltaV2SingleCommit(kernelCommit)
    assert(commit.version === 7L)
    val error = try {
      intercept[IOException] {
        commit.getActionsIterator()
      }
    } finally {
      commit.close()
    }

    assert(error eq failure)
    verify(kernelCommit).close()
  }

  test("action iteration preserves the read failure when resource cleanup fails") {
    val readFailure = new IOException("action iterator read failed")
    val actionCloseFailure = new IOException("action iterator close failed")
    val commitCloseFailure = new IOException("commit close failed")
    val kernelCommit = mock(classOf[KernelCommitActions])
    val kernelActions = mock(classOf[KernelCloseableIterator[KernelColumnarBatch]])
    when(kernelCommit.getVersion()).thenReturn(7L)
    when(kernelCommit.getActions()).thenReturn(kernelActions)
    when(kernelActions.hasNext).thenAnswer { _ => throw readFailure }
    doThrow(actionCloseFailure).when(kernelActions).close()
    doThrow(commitCloseFailure).when(kernelCommit).close()

    val commit = new DeltaV2SingleCommit(kernelCommit)
    val error = intercept[IOException] {
      Utils.tryWithSafeFinally {
        val actions = commit.getActionsIterator()
        Utils.tryWithSafeFinally {
          actions.toVector
        } {
          actions.close()
        }
      } {
        commit.close()
      }
    }

    assert(error eq readFailure)
    assert(error.getSuppressed.length === 2)
    assert(error.getSuppressed.head eq actionCloseFailure)
    assert(error.getSuppressed()(1).getCause eq commitCloseFailure)
    val order = inOrder(kernelActions, kernelCommit)
    order.verify(kernelActions).close()
    order.verify(kernelCommit).close()
  }

  test("cleanup propagates failures after successfully reading actions") {
    Seq((true, false), (false, true), (true, true)).foreach {
      case (failClosingActions, failClosingCommit) =>
        val actionCloseFailure = new IOException("action iterator close failed")
        val commitCloseFailure = new IOException("commit close failed")
        val kernelCommit = mock(classOf[KernelCommitActions])
        val kernelActions = mock(classOf[KernelCloseableIterator[KernelColumnarBatch]])
        when(kernelCommit.getVersion()).thenReturn(7L)
        when(kernelCommit.getActions()).thenReturn(kernelActions)
        if (failClosingActions) {
          doThrow(actionCloseFailure).when(kernelActions).close()
        }
        if (failClosingCommit) {
          doThrow(commitCloseFailure).when(kernelCommit).close()
        }

        val commit = new DeltaV2SingleCommit(kernelCommit)
        val error = intercept[Exception] {
          Utils.tryWithSafeFinally {
            val actions = commit.getActionsIterator()
            Utils.tryWithSafeFinally {
              assert(actions.isEmpty)
            } {
              actions.close()
            }
          } {
            commit.close()
          }
        }

        if (failClosingActions) {
          assert(error eq actionCloseFailure)
        } else {
          assert(error.getCause eq commitCloseFailure)
        }
        if (failClosingActions && failClosingCommit) {
          assert(error.getSuppressed.length === 1)
          assert(error.getSuppressed.head.getCause eq commitCloseFailure)
        } else {
          assert(error.getSuppressed.isEmpty)
        }
        val order = inOrder(kernelActions, kernelCommit)
        order.verify(kernelActions).close()
        order.verify(kernelCommit).close()
    }
  }

  test("opening actions preserves unrelated failures") {
    Seq(
      new IllegalStateException("Cannot read action batches"),
      new IllegalArgumentException("Invalid action batch")
    ).foreach { failure =>
      val kernelCommit = new TestCommitActions(Seq.empty) {
        override def getActions(): KernelCloseableIterator[KernelColumnarBatch] = throw failure
      }
      val commit = DeltaV2SingleCommit(kernelCommit)
      val error = try {
        intercept[RuntimeException] {
          commit.getActionsIterator()
        }
      } finally {
        commit.asInstanceOf[DeltaV2SingleCommit].close()
      }
      assert(error eq failure)
      assert(kernelCommit.closed)
    }
  }

  test("construction closes the Kernel commit when reading its timestamp fails") {
    val failure = new IllegalStateException("Cannot read commit timestamp")
    val kernelCommit = new TestCommitActions(Seq.empty) {
      override def getTimestamp: Long = throw failure
    }
    val error = intercept[IllegalStateException] {
      DeltaV2SingleCommit(kernelCommit)
    }
    assert(error eq failure)
    assert(kernelCommit.closed)
  }
}
