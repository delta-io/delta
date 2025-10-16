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
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.statistics.SnapshotStatistics.ChecksumWriteMode
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.scalatest.funsuite.AnyFunSuite

class SnapshotChecksumStatisticsAndWriteSuite extends AnyFunSuite with TestUtils {

  val testSchema = new StructType().add("id", INTEGER)

  private def assertCrcExistsAtLatest(engine: Engine, tablePath: String): Unit = {
    val latestSnapshot = TableManager.loadSnapshot(tablePath).build(engine)
    assert(latestSnapshot.getStatistics.getChecksumWriteMode === ChecksumWriteMode.NONE)
  }

  test("getChecksumWriteMode: CRC already exists => NONE (trivial case)") {
    withTempDirAndEngine { (tablePath, engine) =>
      // GIVEN
      val txn0 = TableManager.buildCreateTableTransaction(tablePath, testSchema, "x").build(engine)
      val result0 = txn0.commit(engine, emptyIterable())
      val snapshot0 = result0.getPostCommitSnapshot.get()
      snapshot0.writeChecksumSimple(engine)

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
      assert(snapshot0.getStatistics.getChecksumWriteMode == ChecksumWriteMode.SIMPLE) // expected
      snapshot0.writeChecksumSimple(engine) // we can write it!
      assertCrcExistsAtLatest(engine, tablePath) // it exists now
    }
  }

  test("getChecksumWriteMode: CRC exists at N-1 => SIMPLE") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      val txn0 = TableManager.buildCreateTableTransaction(tablePath, testSchema, "xx").build(engine)
      val result0 = txn0.commit(engine, emptyIterable())
      val snapshot0 = result0.getPostCommitSnapshot.get()
      snapshot0.writeChecksumSimple(engine)
      assertCrcExistsAtLatest(engine, tablePath)

      // ===== WHEN =====
      val txn1 = snapshot0.buildUpdateTableTransaction("xx", Operation.WRITE).build(engine)
      val result1 = txn1.commit(engine, emptyIterable())

      // ===== THEN =====
      val snapshot1 = result1.getPostCommitSnapshot.get()
      assert(snapshot1.getStatistics.getChecksumWriteMode == ChecksumWriteMode.SIMPLE) // expected
      snapshot1.writeChecksumSimple(engine) // we can write it!
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
      assert(snapshot1.getStatistics.getChecksumWriteMode == ChecksumWriteMode.FULL) // expected
      snapshot1.writeChecksumFull(engine) // we can write it!
      assertCrcExistsAtLatest(engine, tablePath) // it exists now
    }
  }

  test("getChecksumWriteMode: PostCommitSnapshot (starting from CREATE) => always SIMPLE") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      // We do NOT write 00.crc.
      var txn = TableManager.buildCreateTableTransaction(tablePath, testSchema, "xx").build(engine)
      var postCommitSnapshot = txn.commit(engine, emptyIterable()).getPostCommitSnapshot.get()
      assert(postCommitSnapshot.getStatistics.getChecksumWriteMode === ChecksumWriteMode.SIMPLE)

      // ===== WHEN ====
      for (_ <- 1 to 20) {
        // NOTE: We do NOT write N.crc either!
        txn = postCommitSnapshot.buildUpdateTableTransaction("xx", Operation.WRITE).build(engine)
        postCommitSnapshot = txn.commit(engine, emptyIterable()).getPostCommitSnapshot.get()

        // Nonetheless, our post-commit snapshot (starting from CREATE) should have the CRC info
        // loaded into memory ==> SIMPLE
        assert(postCommitSnapshot.getStatistics.getChecksumWriteMode === ChecksumWriteMode.SIMPLE)
      }

      // ===== THEN =====
      // We can now write 20.crc via the SIMPLE mode, even though 0 to 19.crc do not exist!
      postCommitSnapshot.writeChecksumSimple(engine)
      assertCrcExistsAtLatest(engine, tablePath)
    }
  }

  test("getChecksumWriteMode: PostCommitSnapshot (starting from N>0 with CRC) => always SIMPLE") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      var txn = TableManager.buildCreateTableTransaction(tablePath, testSchema, "xx").build(engine)
      var postCommitSnapshot = txn.commit(engine, emptyIterable()).getPostCommitSnapshot.get()
      assert(postCommitSnapshot.getStatistics.getChecksumWriteMode === ChecksumWriteMode.SIMPLE)

      for (_ <- 1 to 10) {
        txn = postCommitSnapshot.buildUpdateTableTransaction("xx", Operation.WRITE).build(engine)
        postCommitSnapshot = txn.commit(engine, emptyIterable()).getPostCommitSnapshot.get()
      }

      // Versions 0 to 9 do NOT have CRCs. We write 10.crc.
      assert(postCommitSnapshot.getStatistics.getChecksumWriteMode === ChecksumWriteMode.SIMPLE)
      postCommitSnapshot.writeChecksumSimple(engine)

      // Now, we re-start our txn write loop, but using a FRESH Snapshot loaded from version 10.
      var postCommitSnapshot2 = TableManager.loadSnapshot(tablePath).build(engine)

      // ===== WHEN =====
      for (_ <- 11 to 20) {
        // NOTE: We do NOT write N.crc either!
        txn = postCommitSnapshot2.buildUpdateTableTransaction("xx", Operation.WRITE).build(engine)
        postCommitSnapshot2 = txn.commit(engine, emptyIterable()).getPostCommitSnapshot.get()

        // Nonetheless, our post-commit snapshot (starting from a FRESH Snapshot at version 10)
        // should have the CRC info loaded into memory ==> SIMPLE
        assert(postCommitSnapshot2.getStatistics.getChecksumWriteMode === ChecksumWriteMode.SIMPLE)
      }

      // ===== THEN =====
      // We can now write 20.crc via the SIMPLE mode, even though 11 to 19.crc do not exist!
      postCommitSnapshot2.writeChecksumSimple(engine)
      assertCrcExistsAtLatest(engine, tablePath)
    }
  }

  test("invoking writeChecksumSimple when mode is NONE => no-op") {
    withTempDirAndEngine { (tablePath, engine) =>
      val snapshot = TableManager.buildCreateTableTransaction(tablePath, testSchema, "engineInfo")
        .build(engine)
        .commit(engine, emptyIterable())
        .getPostCommitSnapshot.get()
      snapshot.writeChecksumSimple(engine)

      val latestSnapshot = TableManager.loadSnapshot(tablePath).build(engine)
      assert(latestSnapshot.getStatistics.getChecksumWriteMode == ChecksumWriteMode.NONE)
      latestSnapshot.writeChecksumSimple(engine) // no-op, should not throw
    }
  }

  test("invoking writeChecksum**Simple** when mode is FULL => throws") {
    withTempDirAndEngine { (tablePath, engine) =>
      TableManager
        .buildCreateTableTransaction(tablePath, testSchema, "engineInfo")
        .build(engine)
        .commit(engine, emptyIterable())

      val latestSnapshot = TableManager.loadSnapshot(tablePath).build(engine)

      assert(latestSnapshot.getStatistics.getChecksumWriteMode == ChecksumWriteMode.FULL)
      intercept[IllegalStateException] { latestSnapshot.writeChecksumSimple(engine) }
    }
  }

  test("invoking writeChecksumFull when mode is NONE => no-op") {
    withTempDirAndEngine { (tablePath, engine) =>
      val snapshot = TableManager.buildCreateTableTransaction(tablePath, testSchema, "engineInfo")
        .build(engine)
        .commit(engine, emptyIterable())
        .getPostCommitSnapshot.get()
      snapshot.writeChecksumSimple(engine)

      val latestSnapshot = TableManager.loadSnapshot(tablePath).build(engine)
      assert(latestSnapshot.getStatistics.getChecksumWriteMode == ChecksumWriteMode.NONE)

      latestSnapshot.writeChecksumFull(engine) // no-op, should not throw
    }
  }

  test("invoking writeChecksumFull when mode is SIMPLE => succeeds") {
    withTempDirAndEngine { (tablePath, engine) =>
      val snapshot = TableManager.buildCreateTableTransaction(tablePath, testSchema, "x")
        .build(engine).commit(engine, emptyIterable()).getPostCommitSnapshot.get()

      assert(snapshot.getStatistics.getChecksumWriteMode == ChecksumWriteMode.SIMPLE)
      snapshot.writeChecksumFull(engine)
      assertCrcExistsAtLatest(engine, tablePath)
    }
  }
}
