package io.delta.standalone.internal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.delta.flink.source.internal.enumerator.supplier.KernelSnapshotWrapper
import io.delta.kernel.internal.{SnapshotImpl => SnapshotImplKernel}
import io.delta.standalone.DeltaScan
import io.delta.standalone.actions.{AddFile => AddFileJ, Metadata => MetadataJ}
import io.delta.standalone.data.{CloseableIterator, RowRecord => RowParquetRecordJ}
import io.delta.standalone.expressions.Expression
import io.delta.standalone.internal.actions.{AddFile, Metadata, Protocol, SetTransaction}
import io.delta.standalone.internal.scan.DeltaScanImpl
import io.delta.standalone.internal.util.ConversionUtils

/**
 * This class is designed to be passed to OptimisticTransactionImpl, and provide exactly what that
 * needs to operate, but on a Kernel Snapshot rather than a standalone Snapshot.
 *
 * The methods/variables used by OptimisticTransactionImpl that we implement are:
 *  - protocolScala
 *  - metadataScala
 *  - version
 *  - getMetadata
 *
 *  Other functions that are used, we do not implement, but fall back to standalone. These functions
 *  are only called in "exceptional" cases, so should not overly impact performance.
 *  They include:
 *  - scanScala (only called in markFilesAsRead, not used by flink)
 *  - transactions (only used in txnVersion, which is used only on first commit for a flink app)
 *    - This is a val, but it calls setTransactionsScala, so we log for that
 *  - numOfFiles (only used in verifySchemaCompatibility, which happens only when a metadata update occures)
 *  - allFilesScala (only used in verifySchemaCompatibility)
 */
class KernelSnapshotImpl(
    kernelSnapshot: SnapshotImplKernel,
    hadoopConf: Configuration,
    path: Path,
    override val version: Long,
    logSegment: LogSegment,
    minFileRetentionTimestamp: Long,
    deltaLog: DeltaLogImpl,
    timestamp: Long)
  extends SnapshotImpl(hadoopConf, path, version, logSegment, minFileRetentionTimestamp, deltaLog, timestamp) {

  val snapshotWrapper = new KernelSnapshotWrapper(kernelSnapshot)

  /**
   * Internal vals we need to override
   */
  override lazy val protocolScala: Protocol = {
    val kernelProtocol = kernelSnapshot.getProtocol()
    new Protocol(kernelProtocol.getMinReaderVersion(), kernelProtocol.getMinWriterVersion())
  }

  override lazy val metadataScala: Metadata = {
    val metadata = snapshotWrapper.getMetadata()
    ConversionUtils.convertMetadataJ(metadata)
  }

  // Public APIS
  override def getMetadata: MetadataJ = snapshotWrapper.getMetadata()
  override def getVersion: Long = snapshotWrapper.getVersion()

  // Internal apis that we need to verify don't get used often
  override def scanScala(): DeltaScanImpl = {
    logInfo("Calling scanScala on KernelSnapshotImpl")
    super.scanScala()
  }
  override def setTransactionsScala: Seq[SetTransaction] = {
    logInfo("Calling setTransactionsScala on KernelSnapshotImpl")
    super.setTransactionsScala
  }
  override def numOfFiles: Long = {
    logInfo("Calling numOfFiles on KernelSnapshotImpl")
    super.numOfFiles
  }
  override def allFilesScala: Seq[AddFile] = {
    logInfo("Calling allFilesScala on KernelSnapshotImpl")
    super.allFilesScala
  }


  // throw for the following, as we don't expect flink to use them
  override def scan(): DeltaScan = throw new RuntimeException()
  override def scan(predicate: Expression): DeltaScan = throw new RuntimeException()
  override def getAllFiles: java.util.List[AddFileJ] = throw new RuntimeException()
  override def open(): CloseableIterator[RowParquetRecordJ] = throw new RuntimeException()






}
