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

package io.delta.kernel.defaults

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import io.delta.kernel._
import io.delta.kernel.commit.{CommitMetadata, CommitResponse, Committer}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.defaults.utils.{TestCommitterUtils, WriteUtilsWithV2Builders}
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.utils.CloseableIterable.emptyIterable
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class CommitMetadataE2ESuite extends AnyFunSuite
    with WriteUtilsWithV2Builders
    with ParquetSuiteBase
    with TestCommitterUtils {

  private class MockCommitter extends Committer {
    val capturedCommitMetadatas = ListBuffer[CommitMetadata]()

    override def commit(
        engine: Engine,
        finalizedActions: CloseableIterator[Row],
        commitMetadata: CommitMetadata): CommitResponse = {
      capturedCommitMetadatas.append(commitMetadata)

      committerUsingPutIfAbsent.commit(engine, finalizedActions, commitMetadata)
    }
  }

  test("transaction passes domain metadata to committer for add and remove") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      val txn0 = TableManager.buildCreateTableTransaction(tablePath, testSchema, "engineInfo")
        .withTableProperties(Map("delta.feature.domainMetadata" -> "supported").asJava)
        .build(engine)
      commitTransaction(txn0, engine, emptyIterable())

      // ===== WHEN (Case 1: Add domain metadata) =====
      val mockCommitter = new MockCommitter()
      val snapshot1 =
        TableManager.loadSnapshot(tablePath).withCommitter(mockCommitter).build(engine)
      val txn1 = snapshot1.buildUpdateTableTransaction("engineInfo", Operation.WRITE).build(engine)
      txn1.addDomainMetadata("test.domain1", """{"key1":"value1"}""")
      commitTransaction(txn1, engine, emptyIterable())

      // ===== THEN (Case 1: Add domain metadata) =====
      val addedDomainMetadata =
        mockCommitter.capturedCommitMetadatas.head.getNewDomainMetadatas.get(0)
      assert(addedDomainMetadata.getDomain == "test.domain1")
      assert(addedDomainMetadata.getConfiguration == """{"key1":"value1"}""")
      assert(!addedDomainMetadata.isRemoved)

      // ===== WHEN (Case 2: Remove domain metadata) =====
      val snapshot2 =
        TableManager.loadSnapshot(tablePath).withCommitter(mockCommitter).build(engine)
      val txn2 = snapshot2.buildUpdateTableTransaction("engineInfo", Operation.WRITE).build(engine)
      txn2.removeDomainMetadata("test.domain1")
      commitTransaction(txn2, engine, emptyIterable())

      // ===== THEN (Case 2: Remove domain metadata) =====
      val removedDomainMetadata =
        mockCommitter.capturedCommitMetadatas(1).getNewDomainMetadatas.get(0)
      assert(removedDomainMetadata.getDomain == "test.domain1")
      assert(removedDomainMetadata.isRemoved)
    }
  }
}
