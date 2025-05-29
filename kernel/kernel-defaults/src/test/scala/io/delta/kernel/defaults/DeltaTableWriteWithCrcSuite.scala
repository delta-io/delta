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
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.engine.Engine
import io.delta.kernel.hook.PostCommitHook.PostCommitHookType
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.checksum.ChecksumReader
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames.checksumFile
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterable, FileStatus}

/**
 * Trait to mixin into a test suite that extends [[DeltaTableWriteSuiteBase]] to run all the tests
 * with CRC file written after each commit and verify the written CRC files are valid.
 * Note, this requires the test suite uses [[commitTransaction]] and [[verifyWrittenContent]].
 */
trait DeltaTableWriteSuiteBaseWithCrc extends DeltaTableWriteSuiteBase {
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

class DeltaTableWriteWithCrcSuite extends DeltaTableWritesSuite
    with DeltaTableWriteSuiteBaseWithCrc {}

class DeltaReplaceTableWithCrcSuite extends DeltaReplaceTableSuite
    with DeltaTableWriteSuiteBaseWithCrc {}
