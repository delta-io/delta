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
import java.util.Optional

import scala.jdk.CollectionConverters.{collectionAsScalaIterableConverter, setAsJavaSetConverter}

import io.delta.kernel.TransactionCommitResult
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.hook.PostCommitHook.PostCommitHookType
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.checksum.ChecksumReader
import io.delta.kernel.internal.fs.Path

trait CrcTestUtils extends TestUtils {

  def withCrcSimpleExecuted(
      result: TransactionCommitResult,
      engine: Engine): TransactionCommitResult = {
    result.getPostCommitHooks
      .stream()
      .filter(hook => hook.getType == PostCommitHookType.CHECKSUM_SIMPLE)
      .forEach(hook => hook.threadSafeInvoke(engine))
    result
  }

  /** Ensure checksum is readable by CRC reader and correct. */
  def verifyChecksumValid(tablePath: String): Unit = {
    val currentSnapshot = latestSnapshot(tablePath, defaultEngine)
    val checksumVersion = currentSnapshot.getVersion
    val crcInfo = ChecksumReader.getCRCInfo(
      defaultEngine,
      new Path(f"$tablePath/_delta_log/"),
      checksumVersion,
      checksumVersion)
    assert(crcInfo.isPresent)
    // TODO: check metadata, protocol and file size.
    assert(crcInfo.get().getNumFiles
      === collectScanFileRows(currentSnapshot.getScanBuilder.build()).size)
    assert(crcInfo.get().getDomainMetadata === Optional.of(
      currentSnapshot.asInstanceOf[SnapshotImpl].getDomainMetadataMap.values().asScala
        .filterNot(_.isRemoved)
        .toSet
        .asJava))
  }
}
