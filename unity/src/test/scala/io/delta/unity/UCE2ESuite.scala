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

import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.Operation
import io.delta.kernel.Snapshot
import io.delta.kernel.Snapshot.ChecksumWriteMode
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.CloseableIterable
import io.delta.storage.commit.{Commit, GetCommitsResponse}
import io.delta.unity.InMemoryUCClient.TableData

import org.scalatest.funsuite.AnyFunSuite

class UCE2ESuite extends AnyFunSuite with UCCatalogManagedTestUtils {

  /** Commits some data. Verifies UC is updated as expected. Returns the post-commit snapshot. */
  private def writeDataAndVerify(
      engine: Engine,
      snapshot: Snapshot,
      ucClient: InMemoryUCClient,
      expCommitVersion: Long,
      expNumCatalogCommits: Long): Snapshot = {
    val txn = snapshot
      .buildUpdateTableTransaction("engineInfo", Operation.MANUAL_UPDATE)
      .build(engine)
    val result = commitAppendData(engine, txn, seqOfUnpartitionedDataBatch1)
    val tableData = ucClient.getTableDataElseThrow("ucTableId")
    assert(tableData.getMaxRatifiedVersion === expCommitVersion)
    assert(tableData.getCommits.size === expNumCatalogCommits)
    result.getPostCommitSnapshot.get()
  }

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
      result0.getPostCommitSnapshot.get().publish(engine) // Should be no-op!

      // Step 2: WRITE -- v1.uuid.json
      val postCommitSnapshot1 = writeDataAndVerify(
        engine,
        result0.getPostCommitSnapshot.get(),
        ucClient,
        expCommitVersion = 1,
        expNumCatalogCommits = 1)

      // Step 3: WRITE -- v2.uuid.json
      val postCommitSnapshot2 = writeDataAndVerify(
        engine,
        postCommitSnapshot1,
        ucClient,
        expCommitVersion = 2,
        expNumCatalogCommits = 2)

      // Step 4a: PUBLISH v1.json and v2.json -- Note that this does NOT update UC
      postCommitSnapshot2.publish(engine)

      // Step 4b: VERIFY UC is unchanged by the publish operation
      val tableData2 = ucClient.getTableDataElseThrow("ucTableId")
      assert(tableData2.getMaxRatifiedVersion === 2)
      assert(tableData2.getCommits.size === 2)
      postCommitSnapshot2.publish(engine) // idempotent! shouldn't throw

      // Step 5: WRITE -- v3.uuid.json
      // Even though v1.json and v2.json are published, snapshotV2 will still have v1.uuid.json and
      // v2.uuid.json in its LogSegment (since catalog commits take priority). Nonetheless, it will
      // see that v2 is the maxKnownPublishedDeltaVersion. It will include this information in its
      // next commit, and UC will then clean up catalog commits v1.uuid.json and v2.uuid.json.
      val snapshotV2 = loadSnapshot(ucCatalogManagedClient, engine, "ucTableId", tablePath)
      val logSegmentV2 = snapshotV2.getLogSegment
      assert(logSegmentV2.getAllCatalogCommits.asScala.map(x => x.getVersion) === Seq(1, 2))
      assert(logSegmentV2.getMaxPublishedDeltaVersion.get() === 2)
      writeDataAndVerify(
        engine,
        snapshotV2,
        ucClient,
        expCommitVersion = 3,
        expNumCatalogCommits = 1 // just v3.uuid.json, since v1 and v2 are cleaned up
      )

      // Step 6: LOAD -- should read v0.json, v1.json, v2.json, and v3.uuid.json
      val snapshotV3 = loadSnapshot(ucCatalogManagedClient, engine, "ucTableId", tablePath)
      val logSegmentV3 = snapshotV3.getLogSegment
      assert(snapshotV3.getVersion === 3)
      assert(logSegmentV3.getAllCatalogCommits.asScala.map(x => x.getVersion) === Seq(3))
      assert(logSegmentV3.getMaxPublishedDeltaVersion.get() === 2)
    }
  }

  test("can load snapshot for table with CRC files for unpublished versions") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      // ===== GIVEN =====
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

      // CREATE -- v0.json
      val result0 = ucCatalogManagedClient
        .buildCreateTableTransaction("ucTableId", tablePath, testSchema, "test-engine")
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())
      val tableData0 = new TableData(-1, ArrayBuffer[Commit]())
      ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData0)

      var currentSnapshot = result0.getPostCommitSnapshot.get()

      // INSERT -- Empty commits with CRC generation
      for (_ <- 1 to 3) {
        val txn = currentSnapshot
          .buildUpdateTableTransaction("engineInfo", Operation.MANUAL_UPDATE)
          .build(engine)
        val result = txn.commit(engine, CloseableIterable.emptyIterable())
        currentSnapshot = result.getPostCommitSnapshot.get()
        currentSnapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      }

      // ===== WHEN =====
      val freshSnapshot = loadSnapshot(ucCatalogManagedClient, engine, "ucTableId", tablePath)

      // ===== THEN =====
      val logSegment = freshSnapshot.getLogSegment

      assert(freshSnapshot.getVersion === 3)
      assert(logSegment.getAllCatalogCommits.asScala.map(_.getVersion) === Seq(1, 2, 3))
      assert(logSegment.getMaxPublishedDeltaVersion.get() === 0)

      val checksumVersion = FileNames.checksumVersion(logSegment.getLastSeenChecksum.get.getPath)
      assert(checksumVersion === 3)
    }
  }

  test("don't read versions past maxCatalogVersion even if they exist on filesystem") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)

      // Create a custom UCClient that can limit maxRatifiedVersion when needed
      class ConfigurableMaxVersionUCClient extends InMemoryUCClient("ucMetastoreId") {
        @volatile private var maxVersionLimit: Option[Long] = None

        def setMaxVersionLimit(limit: Long): Unit = {
          maxVersionLimit = Some(limit)
        }

        override def getCommits(
            tableId: String,
            tableUri: java.net.URI,
            startVersion: Optional[java.lang.Long],
            endVersion: Optional[java.lang.Long]): GetCommitsResponse = {
          val response = super.getCommits(tableId, tableUri, startVersion, endVersion)
          maxVersionLimit match {
            case Some(limit) =>
              // Filter commits and limit maxRatifiedVersion
              val filteredCommits = response.getCommits.asScala.filter(_.getVersion <= limit)
              new GetCommitsResponse(filteredCommits.asJava, limit)
            case None =>
              response
          }
        }
      }

      val ucClient = new ConfigurableMaxVersionUCClient()
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

      // Step 1: CREATE -- v0.json
      val result0 = ucCatalogManagedClient
        .buildCreateTableTransaction("ucTableId", tablePath, testSchema, "test-engine")
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())
      val tableData0 = new TableData(-1, ArrayBuffer[Commit]())
      ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData0)

      // Step 2: WRITE and commit data up to version 2
      val postCommitSnapshot1 = writeDataAndVerify(
        engine,
        result0.getPostCommitSnapshot.get(),
        ucClient,
        expCommitVersion = 1,
        expNumCatalogCommits = 1)

      val postCommitSnapshot2 = writeDataAndVerify(
        engine,
        postCommitSnapshot1,
        ucClient,
        expCommitVersion = 2,
        expNumCatalogCommits = 2)

      // Step 3: PUBLISH v1.json and v2.json to the filesystem
      postCommitSnapshot2.publish(engine)

      // Step 4: Configure the UC client to limit maxRatifiedVersion to 1
      ucClient.setMaxVersionLimit(1)

      // Step 5: Load snapshot with UC client that limits maxRatifiedVersion to 1
      val snapshot = loadSnapshot(ucCatalogManagedClient, engine, "ucTableId", tablePath)

      // Step 6: Verify that snapshot is at version 1, not version 2
      assert(
        snapshot.getVersion === 1,
        "Snapshot should be at version 1, not reading beyond maxCatalogVersion")

      // Verify the log segment only contains commits up to version 1
      assert(
        snapshot.getLogSegment.getMaxPublishedDeltaVersion.get() === 1,
        "Should recognize published version 1 but not go beyond it")
    }
  }
}
