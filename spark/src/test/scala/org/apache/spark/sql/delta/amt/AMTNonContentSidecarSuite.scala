/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.amt

import java.io.File
import java.nio.file.Files
import java.util.UUID

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaTestUtils, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, Checkpoint, DomainMetadata, SetTransaction, SidecarFile, SidecarType, SingleAction}
import org.apache.spark.sql.delta.util.FileNames


/**
 * Tests of the read path of the AMT non-content (txn and domain metadata) sidecars: snapshot
 * state reconstruction, the incremental AMT writer base, and the read-time checks.
 *
 * No writer produces such sidecars yet, so every test hand-builds them: it writes the sidecar
 * parquet files itself and rebuilds a writer-produced checkpoint with some of its inline entries
 * moved into those sidecars, as another writer may lay them out.
 */
class AMTNonContentSidecarSuite extends AMTCheckpointTestBase {


  /** Runs `body` on a fresh AMT table (one data file, no checkpoint yet). */
  private def withTestTable(name: String)(body: => Unit): Unit = {
    withTable(name) {
      createAMTTable(name, checkpointInterval = Int.MaxValue)
      sql(s"INSERT INTO $name VALUES (1)")
      body
    }
  }

  private def txn(i: Int, version: Long = 1L): SetTransaction =
    SetTransaction(appId = f"app-$i%03d", version = version, lastUpdated = None)

  private def userDomain(i: Int, removed: Boolean = false): DomainMetadata =
    DomainMetadata(domain = f"user.domain$i%03d", configuration = s"""{"i":$i}""", removed)

  private def commit(
      name: String,
      txns: Seq[SetTransaction],
      domains: Seq[DomainMetadata] = Nil): Unit =
    deltaLogForName(name).startTransaction().commit(txns ++ domains, DeltaOperations.ManualUpdate)

  /** Commits an OPTIMIZE CHECKPOINT and returns the Checkpoint action it wrote. */
  private def checkpoint(name: String, incremental: Boolean): Checkpoint = {
    val deltaLog = deltaLogForName(name)
    commitCheckpoint(deltaLog, incremental)
    val version = deltaLog.update().version
    checkpointAt(deltaLog, version).getOrElse(fail(s"No Checkpoint at v$version"))
  }

  /** Whether `domain` is one of the user domains that the tests commit (see [[userDomain]]). */
  private def isUserDomain(domain: String): Boolean = domain.startsWith("user.")

  private def userDomains(dms: Iterable[DomainMetadata]): Set[DomainMetadata] =
    dms.filter(dm => isUserDomain(dm.domain)).toSet

  /**
   * Writes `rows` as one parquet sidecar file of type `sidecarType` under `_delta_log/_sidecars/`
   * and returns its reference. `numEntriesTag` is the count tag to record, None for no tag.
   */
  private def writeSidecar(
      deltaLog: DeltaLog,
      sidecarType: String,
      rows: Seq[SingleAction])(
      numEntriesTag: Option[Long] = Some(rows.size.toLong)): SidecarFile = {
    import org.apache.spark.sql.delta.implicits._
    val sidecarDir = new File(FileNames.sidecarDirPath(deltaLog.logPath).toUri)
    sidecarDir.mkdirs()
    val target = new File(sidecarDir, s"$sidecarType-test-${UUID.randomUUID()}.parquet")
    withTempDir { dir =>
      val out = new File(dir, "out")
      spark.createDataset(rows).coalesce(1).write.parquet(out.getAbsolutePath)
      val parts = out.listFiles().filter(_.getName.endsWith(".parquet"))
      assert(parts.length == 1, s"expected one parquet file, found ${parts.toSeq}")
      Files.copy(parts.head.toPath, target.toPath)
    }
    SidecarFile(
      path = target.getName,
      sizeInBytes = target.length(),
      modificationTime = target.lastModified(),
      tags = numEntriesTag.map(n => Map(AMTNonContentSidecars.NUM_ENTRIES_TAG -> n.toString))
        .orNull,
      `type` = Some(sidecarType))
  }

  private def txnSidecar(deltaLog: DeltaLog, txns: Seq[SetTransaction]): SidecarFile =
    writeSidecar(deltaLog, SidecarType.Type.Txn, txns.map(t => SingleAction(txn = t)))()

  private def domainSidecar(deltaLog: DeltaLog, domains: Seq[DomainMetadata]): SidecarFile =
    writeSidecar(
      deltaLog,
      SidecarType.Type.DomainMetadata,
      domains.map(d => SingleAction(domainMetadata = d)))()

  /** `cp` with its inline txns, inline domain metadata and sidecars replaced. */
  private def rebuilt(
      cp: Checkpoint,
      txns: Seq[SetTransaction],
      domainMetadata: Seq[DomainMetadata],
      sidecars: Seq[SidecarFile]): Checkpoint =
    Checkpoint(
      version = cp.version,
      contentRoot = cp.contentRoot,
      protocol = cp.protocol,
      metaData = cp.metaData,
      domainMetadata = domainMetadata,
      txns = txns,
      sidecars = sidecars)

  /**
   * `cp` rebuilt the way a sidecar writer lays it out: every txn in txn sidecars of at most
   * `perSidecar` entries, every user domain in one domain metadata sidecar, and only the other
   * domains (like the system domains) left inline.
   */
  private def spilled(deltaLog: DeltaLog, cp: Checkpoint, perSidecar: Int = 2): Checkpoint = {
    val (spillableDomains, inlineDomains) =
      cp.domainMetadata.partition(dm => isUserDomain(dm.domain))
    val txnSidecars = cp.txns.sortBy(_.appId).grouped(perSidecar).map(txnSidecar(deltaLog, _))
    val domainSidecars =
      if (spillableDomains.isEmpty) Nil else Seq(domainSidecar(deltaLog, spillableDomains))
    rebuilt(cp, txns = Nil, domainMetadata = inlineDomains,
      sidecars = txnSidecars.toSeq ++ domainSidecars)
  }

  /**
   * `snapshot` served from its AMT checkpoint rewritten by `edit`, as if that checkpoint had been
   * written that way. It has no checksum, so its txns and domains come from state reconstruction.
   */
  private def withEditedCheckpoint(snapshot: Snapshot)(edit: Checkpoint => Checkpoint): Snapshot = {
    val provider = amtProvider(snapshot).getOrElse(fail("expected an AMT checkpoint provider"))
    val edited = AMTCheckpointProvider(
      manifestCommitVersion = provider.manifestCommitVersion,
      checkpointAction = edit(provider.checkpointAction),
      leaves = provider.leaves,
      tableRoot = provider.tableRoot)
    new Snapshot(
      path = snapshot.path,
      version = snapshot.version,
      logSegment = snapshot.logSegment.copy(checkpointProvider = edited),
      deltaLog = snapshot.deltaLog,
      checksumOpt = None
    )
  }

  /** Txns and domains as snapshot state reconstruction sees them (the checkpoint read path). */
  private def reconstructed(snapshot: Snapshot): (Set[SetTransaction], Set[DomainMetadata]) = {
    val actions = snapshot.stateDS.collect()
    (actions.flatMap(a => Option(a.txn)).toSet,
      actions.flatMap(a => Option(a.domainMetadata)).toSet)
  }

  /** Asserts that `actual` holds exactly the txns and domain metadata of `expected`. */
  private def assertSameNonContentState(actual: Snapshot, expected: Snapshot): Unit = {
    assert(actual.setTransactions.toSet == expected.setTransactions.toSet)
    assert(actual.domainMetadata.toSet == expected.domainMetadata.toSet)
    assert(reconstructed(actual) == reconstructed(expected))
  }

  private def loadTxns(deltaLog: DeltaLog, checkpoint: Checkpoint): Seq[SetTransaction] =
    AMTNonContentSidecarReader.loadSidecarActions(spark, deltaLog, checkpoint)
      .getOrElse(fail("expected sidecar actions"))
      .collect()
      .map(_.txn)
      .toSeq

  /** The messages of `error` and all its causes, joined. */
  private def allMessages(error: Throwable): String =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString("\n")

  private def countRecords(logs: Seq[UsageRecord], suffix: String): Int =
    DeltaTestUtils.filterUsageRecords(logs, s"delta.assertions.$suffix").size

  /**
   * Runs the incremental writer directly on `name`'s current AMT with its loaded base edited by
   * `edit`, as if the old checkpoint had been written that way, and returns the new checkpoint
   * (not committed).
   */
  private def writeIncrementalOnEditedBase(
      name: String,
      actionsToCommit: Seq[Action] = Nil)(
      edit: BaseAMTActionsResult => BaseAMTActionsResult): Checkpoint = {
    val deltaLog = deltaLogForName(name)
    val snapshot = deltaLog.update()
    val provider = amtProvider(snapshot).getOrElse(fail("expected an AMT checkpoint provider"))
    val real = new BaseAMTCheckpointActionsProvider(spark, deltaLog, provider)
    val edited = new BaseAMTActionsProvider {
      override def load(): BaseAMTActionsResult = edit(real.load())
    }
    val oldVersion = provider.checkpointAction.contentRoot.version
    new IncrementalAMTWriter(spark, deltaLog).writeIncremental(
      oldAMTActionsProvider = edited,
      intermediateLogCommits =
        snapshot.logSegment.deltas.filter(f => FileNames.getFileVersion(f) > oldVersion),
      attemptVersion = snapshot.version + 1,
      actionsToCommit = actionsToCommit,
      snapshotId = 1L,
      trigger = AMTTriggerMode.CheckpointIntervalIncremental.name).checkpoint
  }

  test("no AMT checkpoint writes non-content sidecars") {
    withTestTable("amt_sidecar_none_written") {
      val name = "amt_sidecar_none_written"
      val txns = (1 to 150).map(txn(_))
      val domains = (1 to 150).map(userDomain(_))
      commit(name, txns, domains)
      Seq(false, true).foreach { incremental =>
        val cp = checkpoint(name, incremental)
        assert(cp.sidecars.isEmpty, s"incremental = $incremental")
        assert(cp.txns.toSet == txns.toSet)
        assert(userDomains(cp.domainMetadata) == domains.toSet)
      }
    }
  }

  test("snapshot state unions inline entries and sidecar entries") {
    withTestTable("amt_sidecar_union") {
      val name = "amt_sidecar_union"
      commit(name, (1 to 5).map(txn(_)), (1 to 4).map(userDomain(_)))
      checkpoint(name, incremental = false)
      val deltaLog = deltaLogForName(name)
      val original = deltaLog.update()
      val edited = withEditedCheckpoint(original)(spilled(deltaLog, _))

      val editedCheckpoint = amtProvider(edited).get.checkpointAction
      assert(editedCheckpoint.txns.isEmpty)
      assert(userDomains(editedCheckpoint.domainMetadata).isEmpty)
      assert(AMTNonContentSidecars.sidecarsOfType(editedCheckpoint, SidecarType.Type.Txn).size == 3)
      assert(AMTNonContentSidecars.sidecarsOfType(
        editedCheckpoint, SidecarType.Type.DomainMetadata).size == 1)
      assert(edited.setTransactions.toSet == (1 to 5).map(txn(_)).toSet)
      assert(userDomains(edited.domainMetadata) == (1 to 4).map(userDomain(_)).toSet)
      assertSameNonContentState(edited, original)
      assert(edited.allFiles.collect().map(_.path).toSet ==
        original.allFiles.collect().map(_.path).toSet)
    }
  }

  test("commits after a checkpoint with sidecars replay over the sidecar entries") {
    withTestTable("amt_sidecar_time_travel") {
      val name = "amt_sidecar_time_travel"
      commit(name, (1 to 5).map(txn(_)), (1 to 4).map(userDomain(_)))
      checkpoint(name, incremental = false)
      val deltaLog = deltaLogForName(name)
      val checkpointCommitVersion = deltaLog.update().version
      // Update a txn and remove a domain that the sidecars carry, and add new entries.
      commit(name, Seq(txn(2, version = 2L), txn(6)), Seq(userDomain(4, removed = true)))
      commit(name, Seq(txn(7)), Seq(userDomain(5)))
      val latest = deltaLog.update()
      // Every version from the checkpoint on is served from the same checkpoint and its sidecars.
      (checkpointCommitVersion to latest.version).foreach { version =>
        val original = deltaLog.getSnapshotAt(version)
        assertSameNonContentState(withEditedCheckpoint(original)(spilled(deltaLog, _)), original)
      }
      val edited = withEditedCheckpoint(latest)(spilled(deltaLog, _))
      assert(edited.setTransactions.toSet ==
        ((1 to 7).filterNot(_ == 2).map(txn(_)) :+ txn(2, version = 2L)).toSet)
      assert(userDomains(edited.domainMetadata) ==
        ((1 to 3) :+ 5).map(userDomain(_)).toSet)
    }
  }

  test("a kind split across inline entries and sidecars is read in full") {
    withTestTable("amt_sidecar_split_kind") {
      val name = "amt_sidecar_split_kind"
      commit(name, (1 to 5).map(txn(_)), (1 to 4).map(userDomain(_)))
      checkpoint(name, incremental = false)
      val deltaLog = deltaLogForName(name)
      val original = deltaLog.update()
      val edited = withEditedCheckpoint(original) { cp =>
        val (inlineTxns, sidecarTxns) = cp.txns.sortBy(_.appId).splitAt(2)
        val (inlineDomains, sidecarDomains) = cp.domainMetadata.partition { dm =>
          !isUserDomain(dm.domain) || dm.domain == "user.domain001"
        }
        rebuilt(cp, txns = inlineTxns, domainMetadata = inlineDomains, sidecars = Seq(
          txnSidecar(deltaLog, sidecarTxns), domainSidecar(deltaLog, sidecarDomains)))
      }
      assertSameNonContentState(edited, original)
    }
  }

  test("a sidecar without the count tag is read") {
    withTestTable("amt_sidecar_untagged") {
      val name = "amt_sidecar_untagged"
      commit(name, txns = (1 to 5).map(txn(_)))
      val cp = checkpoint(name, incremental = false)
      val deltaLog = deltaLogForName(name)
      val untagged = writeSidecar(
        deltaLog, SidecarType.Type.Txn, (1 to 5).map(i => SingleAction(txn = txn(i))))(None)
      assert(AMTNonContentSidecars.numEntries(untagged).isEmpty)
      val withUntagged = rebuilt(cp, txns = Nil, cp.domainMetadata, Seq(untagged))
      assert(loadTxns(deltaLog, withUntagged).toSet == (1 to 5).map(txn(_)).toSet)
      assert(AMTNonContentSidecarReader.readTxnSidecars(deltaLog, withUntagged.sidecars).toSet ==
        (1 to 5).map(txn(_)).toSet)
    }
  }

  test("a row that does not match its sidecar type fails the read") {
    withTestTable("amt_sidecar_wrong_type") {
      val name = "amt_sidecar_wrong_type"
      commit(name, txns = (1 to 3).map(txn(_)))
      val cp = checkpoint(name, incremental = false)
      val deltaLog = deltaLogForName(name)
      // A txn sidecar holding a domain metadata row.
      val wrongType = writeSidecar(deltaLog, SidecarType.Type.Txn,
        Seq(SingleAction(txn = txn(1)), SingleAction(domainMetadata = userDomain(1))))()
      val error = intercept[Exception] {
        loadTxns(deltaLog, rebuilt(cp, Nil, cp.domainMetadata, Seq(wrongType)))
      }
      assert(allMessages(error).contains(
        "AMT non-content sidecar of type txn holds a row without a txn action."), error)
    }
  }

  test("the incremental writer reads the old sidecars and writes their entries inline") {
    withTestTable("amt_sidecar_incremental_base") {
      val name = "amt_sidecar_incremental_base"
      val txns = (1 to 5).map(txn(_))
      val domains = (1 to 4).map(userDomain(_))
      commit(name, txns, domains)
      val cp = checkpoint(name, incremental = false)
      val deltaLog = deltaLogForName(name)
      val spilledCp = spilled(deltaLog, cp)
      sql(s"INSERT INTO $name VALUES (2)")

      val written = writeIncrementalOnEditedBase(name, actionsToCommit = Seq(txn(6))) { base =>
        base.copy(
          inlineSetTransactions = spilledCp.txns,
          inlineDomainMetadatas = spilledCp.domainMetadata,
          nonContentSidecars = spilledCp.sidecars)
      }
      assert(written.sidecars.isEmpty)
      assert(written.txns.toSet == (txns :+ txn(6)).toSet)
      assert(userDomains(written.domainMetadata) == domains.toSet)
    }
  }

  test("the incremental base exposes the old checkpoint's typed sidecars") {
    withTestTable("amt_sidecar_base_field") {
      val name = "amt_sidecar_base_field"
      commit(name, txns = (1 to 3).map(txn(_)))
      checkpoint(name, incremental = false)
      val deltaLog = deltaLogForName(name)
      val original = deltaLog.update()
      val edited = withEditedCheckpoint(original)(spilled(deltaLog, _))
      val editedProvider = amtProvider(edited).get
      val base = new BaseAMTCheckpointActionsProvider(spark, deltaLog, editedProvider).load()
      assert(base.inlineSetTransactions.isEmpty)
      assert(base.nonContentSidecars == editedProvider.checkpointAction.sidecars)
      // Three txns in sidecars of at most two entries.
      assert(AMTNonContentSidecars.sidecarsOfType(base.nonContentSidecars, SidecarType.Type.Txn)
        .size == 2)
    }
  }

  test("a key duplicated across the old checkpoint's inline entries and sidecars fails the " +
      "incremental write") {
    withTestTable("amt_sidecar_writer_duplicate") {
      val name = "amt_sidecar_writer_duplicate"
      commit(name, (1 to 5).map(txn(_)), (1 to 3).map(userDomain(_)))
      val cp = checkpoint(name, incremental = false)
      val deltaLog = deltaLogForName(name)
      val spilledCp = spilled(deltaLog, cp)

      // app-001 is inline (version 9) and in a sidecar (version 1).
      val txnLogs = Log4jUsageLogger.track {
        val error = intercept[IllegalStateException] {
          writeIncrementalOnEditedBase(name) { base =>
            base.copy(
              inlineSetTransactions = Seq(txn(1, version = 9L)),
              inlineDomainMetadatas = spilledCp.domainMetadata,
              nonContentSidecars = spilledCp.sidecars)
          }
        }
        assert(error.getMessage.contains("duplicated txn keys"), error)
        assert(error.getMessage.contains("app-001"), error)
      }
      assert(countRecords(txnLogs, AMTUsageLogs.ALERT_NON_CONTENT_SIDECAR_DUPLICATE_KEY) == 1)

      // user.domain001 is inline and in the domain metadata sidecar.
      val domainError = intercept[IllegalStateException] {
        writeIncrementalOnEditedBase(name) { base =>
          base.copy(
            inlineSetTransactions = Nil,
            inlineDomainMetadatas = spilledCp.domainMetadata :+ userDomain(1),
            nonContentSidecars = spilledCp.sidecars)
        }
      }
      assert(domainError.getMessage.contains("duplicated domainMetadata keys"), domainError)
      assert(domainError.getMessage.contains("user.domain001"), domainError)

      // Without duplicates the same base is written in full.
      val written = writeIncrementalOnEditedBase(name) { base =>
        base.copy(
          inlineSetTransactions = Nil,
          inlineDomainMetadatas = spilledCp.domainMetadata,
          nonContentSidecars = spilledCp.sidecars)
      }
      assert(written.txns.toSet == (1 to 5).map(txn(_)).toSet)
      assert(userDomains(written.domainMetadata) == (1 to 3).map(userDomain(_)).toSet)
    }
  }
}
