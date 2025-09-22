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
import io.delta.kernel.utils.CloseableIterable.emptyIterable
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class CommitMetadataE2ESuite extends AnyFunSuite
    with WriteUtilsWithV2Builders
    with ParquetSuiteBase
    with TestCommitterUtils {

  private class CapturingCommitter extends Committer {
    var latestCommitMetadata: Option[CommitMetadata] = None

    override def commit(
        engine: Engine,
        finalizedActions: CloseableIterator[Row],
        commitMetadata: CommitMetadata): CommitResponse = {
      latestCommitMetadata = Some(commitMetadata)

      committerUsingPutIfAbsent.commit(engine, finalizedActions, commitMetadata)
    }
  }

  test("transaction passes added and removed (and not existing) domain metadatas to committer") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== TEST HELPER SETUP =====
      val capturingCommitter = new CapturingCommitter()

      def createTxnAtLatest(): Transaction =
        TableManager
          .loadSnapshot(tablePath)
          .withCommitter(capturingCommitter)
          .build(engine)
          .buildUpdateTableTransaction("engineInfo", Operation.WRITE)
          .build(engine)

      // ===== GIVEN =====
      val txn0 = TableManager.buildCreateTableTransaction(tablePath, testSchema, "engineInfo")
        .withTableProperties(Map("delta.feature.domainMetadata" -> "supported").asJava)
        .build(engine)
      txn0.addDomainMetadata("foo", "bar")
      commitTransaction(txn0, engine, emptyIterable())

      // ===== WHEN (Case 1: No domain metadata change on table with existing domain metadata) =====
      val txn1 = createTxnAtLatest()
      commitTransaction(txn1, engine, emptyIterable())

      // ===== THEN (Case 1) =====
      {
        val commitMetadata = capturingCommitter.latestCommitMetadata.get
        assert(commitMetadata.getVersion === 1)
        assert(commitMetadata.getCommitDomainMetadatas.isEmpty)
      }

      // ===== WHEN (Case 2: Add domain metadata) =====
      val txn2 = createTxnAtLatest()
      txn2.addDomainMetadata("zip", "zap")
      commitTransaction(txn2, engine, emptyIterable())

      // ===== THEN (Case 2) =====
      {
        val commitMetadata = capturingCommitter.latestCommitMetadata.get
        assert(commitMetadata.getVersion === 2)

        val commitDomainMetadatas = commitMetadata.getCommitDomainMetadatas
        assert(commitDomainMetadatas.size() === 1)

        val dm = commitDomainMetadatas.asScala.head
        assert(dm.getDomain === "zip")
        assert(dm.getConfiguration === "zap")
        assert(!dm.isRemoved)
      }

      // ===== WHEN (Case 3: Remove domain metadata) =====
      val txn3 = createTxnAtLatest()
      txn3.removeDomainMetadata("zip")
      commitTransaction(txn3, engine, emptyIterable())

      // ===== THEN (Case 3: Remove domain metadata) =====
      {
        val commitMetadata = capturingCommitter.latestCommitMetadata.get
        assert(commitMetadata.getVersion === 3)

        val commitDomainMetadatas = commitMetadata.getCommitDomainMetadatas
        assert(commitDomainMetadatas.size() === 1)

        val dm = commitDomainMetadatas.get(0)
        assert(dm.getDomain === "zip")
        assert(dm.isRemoved)
      }

    }
  }

}
