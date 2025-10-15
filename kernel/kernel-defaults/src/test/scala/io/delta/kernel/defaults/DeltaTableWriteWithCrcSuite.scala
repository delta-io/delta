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

import scala.collection.immutable.Seq
import scala.language.implicitConversions

import io.delta.kernel.{Transaction, TransactionCommitResult}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.{TestRow, WriteUtils}
import io.delta.kernel.engine.Engine
import io.delta.kernel.statistics.SnapshotStatistics.ChecksumWriteMode
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable

import org.scalatest.funsuite.AnyFunSuite

/**
 * Trait to mixin into a test suite that extends [[WriteUtils]] to run all the tests
 * with CRC file written after each commit and verify the written CRC files are valid.
 * Note, this requires the test suite uses [[commitTransaction]] and [[verifyWrittenContent]].
 */
trait WriteUtilsWithCrc extends AnyFunSuite with WriteUtils {
  override def commitTransaction(
      txn: Transaction,
      engine: Engine,
      dataActions: CloseableIterable[Row]): TransactionCommitResult = {
    executeCrcSimple(txn.commit(engine, dataActions), engine)
  }

  override def verifyWrittenContent(
      path: String,
      expSchema: StructType,
      expData: Seq[TestRow]): Unit = {
    super.verifyWrittenContent(path, expSchema, expData)
    verifyChecksum(path, expectEmptyTable = expData.isEmpty)
  }
}

/**
 * Trait to mixin into a test suite that extends [[WriteUtils]] to use post-commit snapshots for
 * writing CRC files using the simple CRC write method. This ensures that the checksum write mode is
 * SIMPLE and uses the post-commit snapshot's writeChecksumSimple method. Note, this requires the
 * test suite uses [[commitTransaction]] and [[verifyWrittenContent]].
 */
trait WriteUtilsWithPostCommitSnapshotCrcSimpleWrite extends AnyFunSuite with WriteUtils {

  override def commitTransaction(
      txn: Transaction,
      engine: Engine,
      dataActions: CloseableIterable[Row]): TransactionCommitResult = {
    val txnResult = txn.commit(engine, dataActions)

    val postCommitSnapshot = txnResult
      .getPostCommitSnapshot
      .orElseThrow(() => new IllegalStateException("Required post-commit snapshot is missing"))

    postCommitSnapshot.writeChecksumSimple(engine)

    txnResult
  }

  override def verifyWrittenContent(
      path: String,
      expSchema: StructType,
      expData: Seq[TestRow]): Unit = {
    super.verifyWrittenContent(path, expSchema, expData)
    verifyChecksum(path, expectEmptyTable = expData.isEmpty)
  }
}

class DeltaTableWriteWithCrcSuite extends DeltaTableWritesSuite
    with WriteUtilsWithCrc {}

class DeltaReplaceTableWithCrcSuite extends DeltaReplaceTableSuite
    with WriteUtilsWithCrc {}

class DeltaTableWriteWithPostCommitSnapshotCrcSimpleSuite extends DeltaTableWritesSuite
    with WriteUtilsWithPostCommitSnapshotCrcSimpleWrite {

  // Tests to skip due to known limitation: post-commit snapshots are not yet built after conflicts,
  // so we cannot write CRC files in those cases. See TransactionImpl.buildPostCommitSnapshotOpt.
  // We use `lazy` due to ScalaTest's initialization order.
  lazy val testsToSkip = Set(
    "create table and configure properties with retries",
    "insert into table - idempotent writes",
    "conflicts - concurrent data append (1) after the losing txn has started",
    "conflicts - concurrent data append (5) after the losing txn has started",
    "conflicts - concurrent data append (12) after the losing txn has started")

  override protected def test(
      testName: String,
      testTags: org.scalatest.Tag*)(
      testFun: => Any)(implicit pos: org.scalactic.source.Position): Unit = {
    if (testsToSkip.contains(testName)) {
      ignore(testName, testTags: _*)(testFun)(pos)
    } else {
      super.test(testName, testTags: _*)(testFun)(pos)
    }
  }
}

class DeltaReplaceTableWithPostCommitSnapshotCrcSimpleSuite extends DeltaReplaceTableSuite
    with WriteUtilsWithPostCommitSnapshotCrcSimpleWrite {}
