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
import io.delta.kernel.internal.checksum.ChecksumReader
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable

/**
 * Test suite that run all tests in DeltaTableWritesSuite with CRC file written
 * after each delta commit. This test suite will verify that the written CRC files are valid.
 */
class DeltaTableWriteWithCrcSuite extends DeltaTableWritesSuite {

  override def commitTransaction(
      txn: Transaction,
      engine: Engine,
      dataActions: CloseableIterable[Row]): TransactionCommitResult = {
    val result = txn.commit(engine, dataActions)
    result.getPostCommitHooks
      .stream()
      .filter(hook => hook.getType == PostCommitHookType.CHECKSUM_SIMPLE)
      .forEach(hook => hook.threadSafeInvoke(engine))
    result
  }

  override def verifyWrittenContent(
      path: String,
      expSchema: StructType,
      expData: Seq[TestRow]): Unit = {
    super.verifyWrittenContent(path, expSchema, expData)
    verifyChecksumValid(path)
  }

  /** Ensure checksum is readable by CRC reader. */
  def verifyChecksumValid(
      tablePath: String): Unit = {
    val checksumVersion = latestSnapshot(tablePath, defaultEngine).getVersion
    val crcInfo = ChecksumReader.getCRCInfo(
      defaultEngine,
      new Path(f"$tablePath/_delta_log/"),
      checksumVersion,
      checksumVersion)
    assert(crcInfo.isPresent)
  }
}
