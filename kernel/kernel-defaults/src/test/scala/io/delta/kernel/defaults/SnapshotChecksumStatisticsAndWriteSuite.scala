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

import io.delta.kernel.{Operation, TableManager}
import io.delta.kernel.Snapshot.ChecksumWriteMode
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.scalatest.funsuite.AnyFunSuite

class SnapshotChecksumStatisticsAndWriteSuite extends AnyFunSuite with TestUtils {

  val testSchema = new StructType().add("id", INTEGER)

  private def assertCrcExistsAtLatest(engine: Engine, tablePath: String): Unit = {
    val latestSnapshot = TableManager.loadSnapshot(tablePath).build(engine)
    assert(latestSnapshot.getStatistics.getChecksumWriteMode.isEmpty)
  }

  test("getChecksumWriteMode: CRC already exists => empty (trivial case)") {
    withTempDirAndEngine { (tablePath, engine) =>
      // GIVEN
      val txn0 = TableManager.buildCreateTableTransaction(tablePath, testSchema, "x").build(engine)
      val result0 = txn0.commit(engine, emptyIterable())
      val snapshot0 = result0.getPostCommitSnapshot.get()
      snapshot0.writeChecksum(engine, ChecksumWriteMode.SIMPLE)

      // WHEN/THEN
      assertCrcExistsAtLatest(engine, tablePath) // this is what we are really testing. trivial.
    }
  }

  test("getChecksumWriteMode: created new table => SIMPLE") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== WHEN =====
      val txn0 = TableManager.buildCreateTableTransaction(tablePath, testSchema, "xx").build(engine)
      val result0 = txn0.commit(engine, emptyIterable())

      // ===== THEN =====
      val snapshot0 = result0.getPostCommitSnapshot.get()
      assert(snapshot0.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.SIMPLE)
      snapshot0.writeChecksum(engine, ChecksumWriteMode.SIMPLE) // we can write it!
      assertCrcExistsAtLatest(engine, tablePath) // it exists now
    }
  }

  test("getChecksumWriteMode: CRC exists at N-1 => SIMPLE") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      val txn0 = TableManager.buildCreateTableTransaction(tablePath, testSchema, "xx").build(engine)
      val result0 = txn0.commit(engine, emptyIterable())
      val snapshot0 = result0.getPostCommitSnapshot.get()
      snapshot0.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      assertCrcExistsAtLatest(engine, tablePath)

      // ===== WHEN =====
      val txn1 = snapshot0.buildUpdateTableTransaction("xx", Operation.WRITE).build(engine)
      val result1 = txn1.commit(engine, emptyIterable())

      // ===== THEN =====
      val snapshot1 = result1.getPostCommitSnapshot.get()
      assert(snapshot1.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.SIMPLE)
      snapshot1.writeChecksum(engine, ChecksumWriteMode.SIMPLE) // we can write it!
      assertCrcExistsAtLatest(engine, tablePath) // it exists now
    }
  }

  test("getChecksumWriteMode: CRC gap exists (no CRC at N-1) with fresh Snapshot => FULL") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      val txn0 = TableManager.buildCreateTableTransaction(tablePath, testSchema, "xx").build(engine)
      txn0.commit(engine, emptyIterable()) // We do NOT write 00.crc

      // ===== WHEN =====
      // We explicitly load a fresh Snapshot. If we used the post-commit Snapshot,the mode would be
      // SIMPLE! See the test below.
      val txn1 = TableManager
        .loadSnapshot(tablePath)
        .build(engine)
        .buildUpdateTableTransaction("xx", Operation.WRITE).build(engine)
      val result1 = txn1.commit(engine, emptyIterable())
      val snapshot1 = result1.getPostCommitSnapshot.get()

      // ===== THEN =====
      assert(snapshot1.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.FULL)
      snapshot1.writeChecksum(engine, ChecksumWriteMode.FULL) // we can write it!
      assertCrcExistsAtLatest(engine, tablePath) // it exists now
    }
  }

  // Some additional context: This tests that even if there is no physical CRC file, a post-commit
  // snapshot, and even the 20th post-commit snapshot in a continuous sequence of writes, will still
  // have the CRC info loaded in memory, and thus the mode is SIMPLE.
  test("getChecksumWriteMode: PostCommitSnapshot (starting from CREATE) => always SIMPLE") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      // Create the table and do NOT write 00.crc.
      var txn = TableManager.buildCreateTableTransaction(tablePath, testSchema, "xx").build(engine)
      var postCommitSnapshot = txn.commit(engine, emptyIterable()).getPostCommitSnapshot.get()
      assert(postCommitSnapshot.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.SIMPLE)

      // ===== WHEN ====
      for (_ <- 1 to 20) {
        // NOTE: We do NOT write N.crc either!
        txn = postCommitSnapshot.buildUpdateTableTransaction("xx", Operation.WRITE).build(engine)
        postCommitSnapshot = txn.commit(engine, emptyIterable()).getPostCommitSnapshot.get()

        // Nonetheless, our post-commit snapshot (starting from CREATE) should have the CRC info
        // loaded into memory ==> SIMPLE
        assert(
          postCommitSnapshot.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.SIMPLE)
      }

      // ===== THEN =====
      // We can now write 20.crc via the SIMPLE mode, even though 0 to 19.crc do not exist!
      postCommitSnapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      assertCrcExistsAtLatest(engine, tablePath)
    }
  }

  // Some additional context: This tests that when starting from a fresh snapshot with an existing
  // CRC file (at version 10), all subsequent post-commit snapshots in a continuous sequence will
  // inherit and maintain the CRC info in memory, making the mode SIMPLE even without intermediate
  // CRC files being written.
  test("getChecksumWriteMode: PostCommitSnapshot (starting from N>0 with CRC) => always SIMPLE") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      // Create the table and do NOT write 00.crc.
      var txn = TableManager.buildCreateTableTransaction(tablePath, testSchema, "xx").build(engine)
      var postCommitSnapshot = txn.commit(engine, emptyIterable()).getPostCommitSnapshot.get()
      assert(postCommitSnapshot.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.SIMPLE)

      for (_ <- 1 to 10) {
        // Commit versions 1-10 without writing CRC files
        txn = postCommitSnapshot.buildUpdateTableTransaction("xx", Operation.WRITE).build(engine)
        postCommitSnapshot = txn.commit(engine, emptyIterable()).getPostCommitSnapshot.get()
      }

      // Versions 0 to 9 do NOT have CRCs. Now we write 10.crc.
      assert(postCommitSnapshot.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.SIMPLE)
      postCommitSnapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)

      // Now, we restart our txn write loop, but using a FRESH Snapshot loaded from version 10.
      // It will see the 10.crc file.
      var postCommitSnapshot2 = TableManager.loadSnapshot(tablePath).build(engine)

      // ===== WHEN =====
      for (_ <- 11 to 20) {
        // NOTE: We do NOT write N.crc either!
        txn = postCommitSnapshot2.buildUpdateTableTransaction("xx", Operation.WRITE).build(engine)
        postCommitSnapshot2 = txn.commit(engine, emptyIterable()).getPostCommitSnapshot.get()

        // Nonetheless, our post-commit snapshot (starting from a FRESH Snapshot at version 10)
        // should have the CRC info loaded into memory ==> SIMPLE
        assert(
          postCommitSnapshot2.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.SIMPLE)
      }

      // ===== THEN =====
      // We can now write 20.crc via the SIMPLE mode, even though 11 to 19.crc do not exist!
      postCommitSnapshot2.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      assertCrcExistsAtLatest(engine, tablePath)
    }
  }

  test("invoking writeChecksum with SIMPLE mode when actual mode is FULL => throws") {
    withTempDirAndEngine { (tablePath, engine) =>
      TableManager
        .buildCreateTableTransaction(tablePath, testSchema, "engineInfo")
        .build(engine)
        .commit(engine, emptyIterable())

      val latestSnapshot = TableManager.loadSnapshot(tablePath).build(engine)

      assert(latestSnapshot.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.FULL)

      intercept[IllegalStateException] {
        latestSnapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      }
    }
  }

  test("invoking writeChecksum when checksum already exists => no-op") {
    withTempDirAndEngine { (tablePath, engine) =>
      val snapshot = TableManager.buildCreateTableTransaction(tablePath, testSchema, "engineInfo")
        .build(engine)
        .commit(engine, emptyIterable())
        .getPostCommitSnapshot.get()
      snapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)

      val latestSnapshot = TableManager.loadSnapshot(tablePath).build(engine)
      assert(latestSnapshot.getStatistics.getChecksumWriteMode.isEmpty)

      // Both SIMPLE and FULL should be no-op when checksum already exists
      latestSnapshot.writeChecksum(engine, ChecksumWriteMode.FULL) // no-op, should not throw
      latestSnapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE) // no-op, should not throw
    }
  }

  test("invoking writeChecksum with FULL mode when actual mode is SIMPLE => succeeds") {
    withTempDirAndEngine { (tablePath, engine) =>
      val snapshot = TableManager.buildCreateTableTransaction(tablePath, testSchema, "x")
        .build(engine).commit(engine, emptyIterable()).getPostCommitSnapshot.get()

      assert(snapshot.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.SIMPLE)
      snapshot.writeChecksum(engine, ChecksumWriteMode.FULL)
      assertCrcExistsAtLatest(engine, tablePath)
    }
  }

  // Note that we can only use SIMPLE when starting from a post-commit snapshot whose transaction
  // started with a CRC file. Even if there's a CRC file at historical version N-1, we still need to
  // do a FULL replay to load the CRC file at version N to write it.
  test("write checksum at historical version => FULL mode") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create version 0 without writing its CRC
      val txn0 = TableManager.buildCreateTableTransaction(tablePath, testSchema, "xx").build(engine)
      txn0.commit(engine, emptyIterable())

      // Create version 1 without writing its CRC
      val snapshot0 = TableManager.loadSnapshot(tablePath).build(engine)
      val txn1 = snapshot0.buildUpdateTableTransaction("xx", Operation.WRITE).build(engine)
      txn1.commit(engine, emptyIterable())

      // Create version 2 without writing its CRC
      val snapshot1 = TableManager.loadSnapshot(tablePath).build(engine)
      val txn2 = snapshot1.buildUpdateTableTransaction("xx", Operation.WRITE).build(engine)
      txn2.commit(engine, emptyIterable())

      // Now load the historical snapshot at version 1 and check its mode
      val historicalSnapshot = TableManager.loadSnapshot(tablePath).atVersion(1).build(engine)
      assert(historicalSnapshot.getVersion == 1)
      assert(historicalSnapshot.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.FULL)

      // Write checksum for the historical version 1
      historicalSnapshot.writeChecksum(engine, ChecksumWriteMode.FULL)

      // Verify CRC file exists for version 1
      val snapshot1Again = TableManager.loadSnapshot(tablePath).atVersion(1).build(engine)
      assert(snapshot1Again.getStatistics.getChecksumWriteMode.isEmpty)
    }
  }

  test("concurrent checksum write => second write still returns successfully without error") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      // Step 1: Create a table (v0.json) and get the post-commit snapshot
      val txn = TableManager.buildCreateTableTransaction(tablePath, testSchema, "xx").build(engine)
      val result = txn.commit(engine, emptyIterable())
      val postCommitSnapshot = result.getPostCommitSnapshot.get()

      // Step 2: Load a new snapshot to latest (v0)
      val freshSnapshot = TableManager.loadSnapshot(tablePath).build(engine)
      assert(freshSnapshot.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.FULL)

      // ===== WHEN =====
      // Step 3: Use the fresh snapshot to write the checksum
      freshSnapshot.writeChecksum(engine, ChecksumWriteMode.FULL)
      assertCrcExistsAtLatest(engine, tablePath)

      // ===== THEN =====
      // Step 4: Use the first (post-commit) snapshot to write the checksum -- should NOT fail
      // This simulates a concurrent write scenario where another writer already wrote the CRC
      assert(postCommitSnapshot.getStatistics.getChecksumWriteMode.get == ChecksumWriteMode.SIMPLE)
      postCommitSnapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE) // should be a no-op
    }
  }
}
