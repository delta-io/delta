/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.unity

import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.Operation
import io.delta.kernel.utils.CloseableIterable
import io.delta.storage.commit.Commit
import io.delta.unity.InMemoryUCClient.TableData

import org.scalatest.funsuite.AnyFunSuite

class UCE2ESuite extends AnyFunSuite with UCCatalogManagedTestUtils {
  test("simple case: create, write, publish, load") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

      // Step 1: CREATE -- v0.json
      val result0 = ucCatalogManagedClient
        .buildCreateTableTransaction("ucTableId", tablePath, testSchema, "test-engine")
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable() /* dataActions */ )
      val tableData0 = new TableData(-1, ArrayBuffer[Commit]())
      ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData0)

      // Step 2: WRITE -- v1.uuid.json
      val txn1 = result0.getPostCommitSnapshot.get()
        .buildUpdateTableTransaction("engineInfo", Operation.MANUAL_UPDATE)
        .build(engine)
      val result1 = commitAppendData(engine, txn1, seqOfUnpartitionedDataBatch1)
      val tableData1 = ucClient.getTableDataElseThrow("ucTableId")
      assert(tableData1.getMaxRatifiedVersion === 1)
      assert(tableData1.getCommits.size === 1)

      // Step 3: WRITE (again) -- v2.uuid.json
      val txn2 = result1.getPostCommitSnapshot.get()
        .buildUpdateTableTransaction("engineInfo", Operation.MANUAL_UPDATE)
        .build(engine)
      val result2 = commitAppendData(engine, txn2, seqOfUnpartitionedDataBatch2)
      val tableData2 = ucClient.getTableDataElseThrow("ucTableId")
      assert(tableData2.getMaxRatifiedVersion === 2)
      assert(tableData2.getCommits.size === 2)

      // Step 4: PUBLISH
      result2.getPostCommitSnapshot.get().publish(engine) // Note: this does NOT update UC
      tableData2.forceRemoveCommitsUpToVersion(2)
      assert(tableData2.getMaxRatifiedVersion === 2)
      assert(tableData2.getCommits.isEmpty)
      result2.getPostCommitSnapshot.get().publish(engine) // idempotent! shouldn't throw

      // Step 5: LOAD -- should read v0.json, v1.json, v2.json (no catalog commits)
      val snapshotV2 = loadSnapshot(ucCatalogManagedClient, engine, "ucTableId", tablePath)
      assert(snapshotV2.getVersion === 2)
      assert(snapshotV2.getLogSegment.getMaxPublishedDeltaVersion.get() === 2)
      snapshotV2.publish(engine) // nothing to publish! shouldn't throw
    }
  }
}
