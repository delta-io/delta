package io.delta.standalone.internal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory;

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
 * Utility class for transactions method
 */
class AppIdDeferedMap(snapshot: SnapshotImplKernel) extends Map[String, Long] {
  def get(applicationId: String): Option[Long] = {
    AppIdDeferedMap.LOG.info("Starting getRecentTransactionVersion")
    val start = System.nanoTime()
    val versionJOpt = snapshot.getRecentTransactionVersion(applicationId)
    val timeElapsed = System.nanoTime() - start
    if (versionJOpt.isPresent) {
      AppIdDeferedMap.LOG.info("got recent version in " + timeElapsed)
      Some(versionJOpt.get)
    } else {
      AppIdDeferedMap.LOG.info("discovered doesn't exist in " + timeElapsed)
      None
    }
  }

  // we don't support iterating, so return an empty one (so that printing etc can work)
  def iterator: Iterator[(String, Long)] = Iterator()

  // we don't support add/remove, so throw exceptions for those
  def +[V1 >: Long](kv: (String, V1)): Map[String,V1] = throw new RuntimeException()
  def -(key: String): Map[String,Long] = throw new RuntimeException()
}

object AppIdDeferedMap {
  val LOG = LoggerFactory.getLogger(classOf[AppIdDeferedMap]);
}

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
class KernelSnapshotDelegator(
    kernelSnapshot: SnapshotImplKernel,
    hadoopConf: Configuration,
    path: Path,
    override val version: Long,
    kernelDeltaLog: KernelDeltaLogDelegator,
    standaloneDeltaLog: DeltaLogImpl)
  extends SnapshotImpl(hadoopConf, path, -1, LogSegment.empty(path), -1, standaloneDeltaLog, -1) {

  val snapshotWrapper = new KernelSnapshotWrapper(kernelSnapshot)
  lazy val standaloneSnapshot: SnapshotImpl = standaloneDeltaLog.getSnapshotForVersionAsOf(getVersion())

  /**
   * Internal vals we need to override
   */
  override lazy val protocolScala: Protocol = {
    if (kernelSnapshot.isInstanceOf[InitialKernelSnapshotImpl]) {
      Protocol()
    } else {
      val kernelProtocol = kernelSnapshot.getProtocol()
      new Protocol(kernelProtocol.getMinReaderVersion(), kernelProtocol.getMinWriterVersion())
    }
  }

  override lazy val metadataScala: Metadata = {
    if (kernelSnapshot.isInstanceOf[InitialKernelSnapshotImpl]) {
      Metadata()
    } else {
      val metadata = snapshotWrapper.getMetadata()
      ConversionUtils.convertMetadataJ(metadata)
    }
  }

  // we provide a more efficient version of getting transactions
  override lazy val transactions: Map[String, Long] = {
    if (kernelSnapshot.isInstanceOf[InitialKernelSnapshotImpl]) {
      Map()
    } else {
      // we use an optimized version to quickly read the most recent appId, but, since
      // OptimisticTransactionImpl hasn't told us what appId it wants here yet, we return a map that
      // will do the work when requested and that information is available
      new AppIdDeferedMap(kernelSnapshot)
    }
  }


  // Public APIS
  override def getMetadata: MetadataJ = snapshotWrapper.getMetadata()
  override def getVersion: Long = snapshotWrapper.getVersion()

  // Internal apis that we need to verify don't get used often
  override def scanScala(): DeltaScanImpl = {
    logInfo("Calling scanScala on KernelSnapshotDelegator")
    standaloneSnapshot.scanScala()
  }
  override def setTransactionsScala: Seq[SetTransaction] = {
    logInfo("Calling setTransactionsScala on KernelSnapshotDelegator")
    standaloneSnapshot.setTransactionsScala
  }
  override def numOfFiles: Long = {
    if (kernelSnapshot.isInstanceOf[InitialKernelSnapshotImpl]) {
      0L
    } else {
      logInfo("Calling numOfFiles on KernelSnapshotDelegator")
      standaloneSnapshot.numOfFiles
    }
  }
  override def allFilesScala: Seq[AddFile] = {
    logInfo("Calling allFilesScala on KernelSnapshotDelegator")
    standaloneSnapshot.allFilesScala
  }


  // throw for the following, as we don't expect flink to use them
  override def scan(): DeltaScan = throw new RuntimeException()
  override def scan(predicate: Expression): DeltaScan = throw new RuntimeException()
  override def getAllFiles: java.util.List[AddFileJ] = throw new RuntimeException()
  override def open(): CloseableIterator[RowParquetRecordJ] = throw new RuntimeException()
}
