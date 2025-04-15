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
