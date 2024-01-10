package io.delta.standalone.internal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.delta.kernel.{Table, TableNotFoundException}
import io.delta.kernel.defaults.client.DefaultTableClient
import io.delta.kernel.internal.{SnapshotImpl => SnapshotImplKernel, TableImpl}
import io.delta.standalone.VersionLog
import io.delta.standalone.actions.{CommitInfo => CommitInfoJ}
import io.delta.standalone.internal.{SnapshotImpl => StandaloneSnapshotImpl, InitialSnapshotImpl => StandaloneInitialSnapshotImpl}
import io.delta.standalone.internal.util.{Clock, SystemClock}

class KernelOptTxn(deltaLog: DeltaLogImpl, snapshot: KernelSnapshotDelegator)
    extends OptimisticTransactionImpl(deltaLog, snapshot) {
  override def txnVersion(applicationId: String): Long = {
    readTxn += applicationId
    snapshot.getLatestTransactionVersion(applicationId).getOrElse(-1L)
  }
}


/**
 * We want to be able to construct an OptimisticTransactionImpl that uses a delta log and a snapshot
 * provided by the Delta Kernel. OptimisticTransactionImpl takes a DeltaLogImpl and SnapshotImpl
 * internally, so we need classes that extend those, and this is the one for DeltaLogImpl. It
 * provides features used by flink+startTransaction.
 */
class KernelDeltaLogDelegator(
    tableClient: DefaultTableClient,
    table: TableImpl,
    standaloneDeltaLog: DeltaLogImpl,
    hadoopConf: Configuration,
    logPath: Path,
    dataPath: Path,
    clock: Clock)
  extends DeltaLogImpl(hadoopConf, logPath, dataPath, clock) {

  // We override this so our super DeltaLogImpl constructor doesn't actually try and read the log
  // none of the delegated methods require access to the current snapshot, so it's safe to just have
  // this be null
  override def getSnapshotAtInit(): SnapshotImpl = null

  var currKernelSnapshot: Option[KernelSnapshotDelegator] = None

  override def snapshot(): StandaloneSnapshotImpl = { // but is actually a KernelSnapshotDelegator
    if (currKernelSnapshot.isEmpty) { return update() }
    return currKernelSnapshot.get
  }

  override def update(): StandaloneSnapshotImpl = { // but is actually a KernelSnapshotDelegator
    // get latest snapshot via kernel
    val kernelSnapshot = try {
      table.getLatestSnapshot(tableClient).asInstanceOf[SnapshotImplKernel]
    } catch {
      case e: TableNotFoundException =>
        return new StandaloneInitialSnapshotImpl(hadoopConf, logPath, this)
    }
    currKernelSnapshot = Some(new KernelSnapshotDelegator(
      kernelSnapshot,
      hadoopConf,
      logPath,
      kernelSnapshot.getVersion(tableClient), // note: tableClient isn't used
      this,
      standaloneDeltaLog
    ))
    currKernelSnapshot.get
  }

  override def startTransaction(): io.delta.standalone.OptimisticTransaction = {
    val snapshot = update()
    if (snapshot.isInstanceOf[KernelSnapshotDelegator]) {
      new KernelOptTxn(this, snapshot.asInstanceOf[KernelSnapshotDelegator])
    } else {
      new OptimisticTransactionImpl(this, snapshot)
    }
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
  def forTable(hadoopConf: Configuration, dataPath: String): KernelDeltaLogDelegator = {
    val rawPath = new Path(dataPath, "_delta_log")
    val fs = rawPath.getFileSystem(hadoopConf)
    val logPath = fs.makeQualified(rawPath)
    val dataPathFromLog = logPath.getParent
    val clock = new SystemClock
    // Create this first as we use it to it create the specified table if it doesn't exist, which
    // the kernel does not
    val standaloneDeltaLog = new DeltaLogImpl(hadoopConf, logPath, dataPathFromLog, clock)
    standaloneDeltaLog.ensureLogDirectoryExist()
    val tableClient = DefaultTableClient.create(hadoopConf)
    val table = Table.forPath(tableClient, dataPath).asInstanceOf[TableImpl]
    // Todo: Potentially we could get the resolved paths out of the table above
    new KernelDeltaLogDelegator(tableClient, table, standaloneDeltaLog, hadoopConf, logPath, dataPathFromLog, clock)
  }
}
