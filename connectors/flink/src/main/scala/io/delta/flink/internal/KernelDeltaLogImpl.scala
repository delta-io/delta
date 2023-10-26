package io.delta.standalone.internal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.delta.kernel.Table
import io.delta.kernel.defaults.client.DefaultTableClient
import io.delta.kernel.internal.{SnapshotImpl => SnapshotImplKernel, TableImpl}
import io.delta.standalone.VersionLog
import io.delta.standalone.actions.{CommitInfo => CommitInfoJ}
import io.delta.standalone.internal.{SnapshotImpl => StandaloneSnapshotImpl}
import io.delta.standalone.internal.util.{Clock, SystemClock}

/**
 * We want to be able to construct an OptimisticTransactionImpl that uses a delta log and a snapshot
 * provided by the Delta Kernel. OptimisticTransactionImpl takes a DeltaLogImpl and SnapshotImpl
 * internally, so we need classes that extend those, and this is the one for DeltaLogImpl. It
 * provides features used by flink+startTransaction.
 */
class KernelDeltaLogImpl(
    tableClient: DefaultTableClient,
    table: TableImpl,
    hadoopConf: Configuration,
    logPath: Path,
    dataPath: Path,
    clock: Clock)
  extends DeltaLogImpl(hadoopConf, logPath, dataPath, clock) {

  var currKernelSnapshot: Option[KernelSnapshotImpl] = None

  override def snapshot(): StandaloneSnapshotImpl = { // but is actually a KernelSnapshotImpl
    if (currKernelSnapshot.isEmpty) { update() }
    return currKernelSnapshot.get
  }

  override def update(): StandaloneSnapshotImpl = { // but is actually a KernelSnapshotImpl
    // get latest snapshot via kernel
    val kernelSnapshot = table.getLatestSnapshot(tableClient).asInstanceOf[SnapshotImplKernel]
    currKernelSnapshot = Some(new KernelSnapshotImpl(
      kernelSnapshot,
      hadoopConf,
      logPath,
      kernelSnapshot.getVersion(tableClient), // note: tableClient isn't used
      LogSegment.empty(logPath),
      -1,
      this,
      -1
    ))
    currKernelSnapshot.get
  }

  override def startTransaction(): io.delta.standalone.OptimisticTransaction = {
    val snapshot = update()
    new OptimisticTransactionImpl(this, snapshot)
  }

  override def tableExists: Boolean = snapshot.version >= 0

  override def getChanges(startVersion: Long, failOnDataLoss: Boolean): java.util.Iterator[VersionLog] = {
    logWarning("KernelDeltaLogImpl falling back to DeltaLogImpl for getChanges")
    return super.getChanges(startVersion, failOnDataLoss);
  }

  override def getSnapshotForVersionAsOf(version: Long): StandaloneSnapshotImpl = {
    throw new RuntimeException()
  }
  override def getSnapshotForTimestampAsOf(timestamp: Long): StandaloneSnapshotImpl = {
    throw new RuntimeException()
  }
  override def getCommitInfoAt(version: Long): CommitInfoJ = {
    throw new RuntimeException()
  }
}

object KernelDeltaLogImpl {
  def forTable(hadoopConf: Configuration, dataPath: String): KernelDeltaLogImpl = {
    val tableClient = DefaultTableClient.create(hadoopConf)
    val table = Table.forPath(tableClient, dataPath).asInstanceOf[TableImpl]
    // Todo: Potentially we could get the resolved paths out of the table above
    val rawPath = new Path(dataPath, "_delta_log")
    val fs = rawPath.getFileSystem(hadoopConf)
    val logPath = fs.makeQualified(rawPath)
    new KernelDeltaLogImpl(tableClient, table, hadoopConf, logPath, logPath.getParent, new SystemClock)
  }
}
