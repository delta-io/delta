package io.delta.standalone.internal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory;

import io.delta.kernel.{Table, TableNotFoundException}
import io.delta.kernel.defaults.client.DefaultTableClient
import io.delta.kernel.internal.{SnapshotImpl => SnapshotImplKernel, TableImpl}
import io.delta.kernel.internal.fs.{Path => KernelPath}
import io.delta.standalone.VersionLog
import io.delta.standalone.actions.{CommitInfo => CommitInfoJ}
import io.delta.standalone.internal.{SnapshotImpl => StandaloneSnapshotImpl}
import io.delta.standalone.internal.util.{Clock, SystemClock}

/**
 *  Utility class to represent an "initial" snapshot as a kernel snapshot. This is equivalent to
 *  InitialSnapshotImpl in standalone land.
 */
class InitialKernelSnapshotImpl(logPath: Path, dataPath: Path, tableClient: DefaultTableClient)
    extends SnapshotImplKernel(
      new KernelPath(logPath.toString()),
      new KernelPath(dataPath.toString()),
      -1,
      io.delta.kernel.internal.snapshot.LogSegment.empty(new KernelPath(logPath.toString())),
      tableClient,
      -1)

/**
 * We want to be able to construct an OptimisticTransactionImpl that uses a delta log and a snapshot
 * provided by the Delta Kernel. OptimisticTransactionImpl takes a DeltaLogImpl and SnapshotImpl
 * internally, so we need classes that extend those, and this is the one for DeltaLogImpl. It
 * provides features used by flink+startTransaction.
 */
class KernelDeltaLogDelegator(
    tableClient: DefaultTableClient,
    table: TableImpl,
    hadoopConf: Configuration,
    logPath: Path,
    dataPath: Path,
    clock: Clock)
  extends DeltaLogImpl(hadoopConf, logPath, dataPath, clock) {

  val standaloneDeltaLog = new DeltaLogImpl(hadoopConf, logPath, dataPath, clock)
  var currKernelSnapshot: Option[KernelSnapshotDelegator] = None

  override def snapshot(): StandaloneSnapshotImpl = { // but is actually a KernelSnapshotDelegator
    if (currKernelSnapshot.isEmpty) { update() }
    return currKernelSnapshot.get
  }

  override def update(): StandaloneSnapshotImpl = { // but is actually a KernelSnapshotDelegator
    // get latest snapshot via kernel
    val totalS = System.nanoTime()
    val kernelSnapshot = try {
      val start = System.nanoTime()
      val sn = table.getLatestSnapshot(tableClient).asInstanceOf[SnapshotImplKernel]
      val timeElapsed = System.nanoTime() - start
      KernelDeltaLogDelegator.LOG.info("Kernel updated snapshot in: " + timeElapsed)
      sn
    } catch {
      case e: TableNotFoundException =>
        new InitialKernelSnapshotImpl(logPath, dataPath, tableClient)
    }
    currKernelSnapshot = Some(new KernelSnapshotDelegator(
      kernelSnapshot,
      hadoopConf,
      logPath,
      kernelSnapshot.getVersion(tableClient), // note: tableClient isn't used
      this,
      standaloneDeltaLog
    ))
    val totalE = System.nanoTime() - totalS
    KernelDeltaLogDelegator.LOG.info("    Total kernel update in: " + totalE)
    currKernelSnapshot.get
  }

  override def startTransaction(): io.delta.standalone.OptimisticTransaction = {
    val snapshot = update()
    new OptimisticTransactionImpl(this, snapshot)
  }

  override def tableExists: Boolean = snapshot.version >= 0

  override def getChanges(startVersion: Long, failOnDataLoss: Boolean): java.util.Iterator[VersionLog] = {
    logWarning("KernelDeltaLogDelegator falling back to DeltaLogImpl for getChanges")
    standaloneDeltaLog.getChanges(startVersion, failOnDataLoss)
  }

  override def getSnapshotForVersionAsOf(version: Long): StandaloneSnapshotImpl = {
    logWarning("KernelDeltaLogDelegator falling back to DeltaLogImpl for getSnapshotForVersionAsOf")
    standaloneDeltaLog.getSnapshotForVersionAsOf(version)
  }
  override def getSnapshotForTimestampAsOf(timestamp: Long): StandaloneSnapshotImpl = {
    throw new RuntimeException()
  }
  override def getCommitInfoAt(version: Long): CommitInfoJ = {
    throw new RuntimeException()
  }
}

object KernelDeltaLogDelegator {
  val LOG = LoggerFactory.getLogger(classOf[KernelDeltaLogDelegator]);

  def forTable(hadoopConf: Configuration, dataPath: String): KernelDeltaLogDelegator = {
    val tableClient = DefaultTableClient.create(hadoopConf)
    val table = Table.forPath(tableClient, dataPath).asInstanceOf[TableImpl]
    // Todo: Potentially we could get the resolved paths out of the table above
    val rawPath = new Path(dataPath, "_delta_log")
    val fs = rawPath.getFileSystem(hadoopConf)
    val logPath = fs.makeQualified(rawPath)
    new KernelDeltaLogDelegator(tableClient, table, hadoopConf, logPath, logPath.getParent, new SystemClock)
  }
}
