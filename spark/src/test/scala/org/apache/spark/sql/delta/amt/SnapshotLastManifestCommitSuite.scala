/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.{DeltaLog, DeltaTestUtils, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, CommitInfo, LastManifestCommit}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, FileNames, JsonUtils}

import org.apache.spark.SparkConf

/**
 * Tests that [[LastManifestCommit]] is surfaced reliably by [[Snapshot.lastManifestCommitOpt]].
 */
trait SnapshotLastManifestCommitSuiteBase extends AMTCheckpointTestBase {

  /** Whether this suite runs with `.crc` files enabled, read from the effective conf. */
  protected def writeChecksumEnabled: Boolean =
    spark.conf.get(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED)

  /**
   * Seeds `lmc` onto every source that exists for this suite's mode. Always rewrites the delta-file
   * `CommitInfo`; the CRC-enabled suite additionally rewrites the `.crc`.
   */
  protected def injectLmc(deltaLog: DeltaLog, version: Long, lmc: Option[LastManifestCommit]): Unit

  /** The usage log emitted by the protocol/metadata reconstruction query. */
  private val protocolAndMetadataQueryLog = "delta.snapshot.protocolAndMetadataQuery"

  /** Rewrites the `CommitInfo` at `version` to carry `lmc`, preserving all other actions. */
  protected def injectLmcIntoCommitInfo(
      deltaLog: DeltaLog, version: Long, lmc: Option[LastManifestCommit]): Unit = {
    val commitPath = DeltaCommitFileProvider(deltaLog.unsafeVolatileSnapshot).deltaFile(version)
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val rewritten = deltaLog.store.readAsIterator(commitPath, hadoopConf).toList
      .map(Action.fromJson)
      .map {
        case ci: CommitInfo => ci.copy(lastManifestCommit = lmc).json
        case other => other.json
      }
    deltaLog.store.write(commitPath, rewritten.toIterator, overwrite = true, hadoopConf)
  }

  /** Rewrites the CRC at `version` to carry `lmc`. Requires the CRC to already exist. */
  protected def injectLmcIntoCrc(
      deltaLog: DeltaLog, version: Long, lmc: Option[LastManifestCommit]): Unit = {
    val checksum = deltaLog.readChecksum(version).getOrElse(
      fail(s"Expected a CRC at version $version to rewrite."))
    val rewritten = JsonUtils.toJson(checksum.copy(lastManifestCommit = lmc))
    deltaLog.store.write(
      FileNames.checksumFile(deltaLog.logPath, version),
      Seq(rewritten).toIterator,
      overwrite = true,
      deltaLog.newDeltaHadoopConf())
  }

  /** Drops the [[DeltaLog]] cache and cold-loads the snapshot at `version` for a fresh read. */
  protected def freshSnapshotAt(tableName: String, version: Long): Snapshot = {
    DeltaLog.clearCache()
    deltaLogForName(tableName).getSnapshotAt(version)
  }

  /**
   * Asserts that `opType` was (or was not) emitted among the in-scope captured `usageLogs`. Wrap
   * the code under test in `implicit val usageLogs = Log4jUsageLogger.track { ... }` first.
   */
  protected def assertUsageLog(opType: String, isPresent: Boolean)(
      implicit usageLogs: Seq[UsageRecord]): Unit = {
    val emitted = DeltaTestUtils.filterUsageRecords(usageLogs, opType).nonEmpty
    assert(emitted == isPresent,
      s"Expected usage log '$opType' ${if (isPresent) "to" else "not to"} be emitted.")
  }

  test("lastManifestCommitOpt resolves via reconstruction across commits with trailing deltas") {
    withTable("amt_lmc_reconstruction") {
      val name = "amt_lmc_reconstruction"
      createAMTTable(name, checkpointInterval = 100)
      sql(s"INSERT INTO $name VALUES (1)")
      sql(s"INSERT INTO $name VALUES (2)")
      sql(s"INSERT INTO $name VALUES (3)")

      val deltaLog = deltaLogForName(name)
      val lmcV1 = LastManifestCommit(version = 1, contentRootVersion = 1)
      val lmcV3 = LastManifestCommit(version = 3, contentRootVersion = 3)
      injectLmc(deltaLog, version = 1, lmc = Some(lmcV1)) // v1: carries a reference.
      // v2: no reference.
      injectLmc(deltaLog, version = 3, lmc = Some(lmcV3)) // v3: carries a different reference.

      /** Assert each snapshot resolves to its own version's reference with the expected path. */
      def assertResolves(version: Long, expected: Option[LastManifestCommit]): Unit = {
        implicit val usageLogs: Seq[UsageRecord] = Log4jUsageLogger.track {
          assert(freshSnapshotAt(name, version).lastManifestCommitOpt == expected)
        }
        // There are trailing deltas, so the CommitInfo fallback is never reached.
        assertUsageLog(AMTUsageLogs.LAST_MANIFEST_COMMIT_READ_FROM_COMMIT_INFO, isPresent = false)
        // The reconstruction query runs iff there is no CRC fast-path to serve the reference.
        assertUsageLog(protocolAndMetadataQueryLog, isPresent = !writeChecksumEnabled)
      }

      assertResolves(1, Some(lmcV1))
      // v2 carries no reference, and v1's must not leak into v2's reconstruction.
      assertResolves(2, None)
      assertResolves(3, Some(lmcV3))
    }
  }

  test("in-commit-timestamp and last-manifest-commit both resolve when present together") {
    // The in-commit-timestamp and last-manifest-commit are sibling fields in CommitInfo, so a
    // single commit carries both on one reconstruction row; both must survive. Catalog-managed
    // tables have in-commit timestamps enabled by default, so no explicit enablement is needed.
    val lmc = LastManifestCommit(version = 7, contentRootVersion = 5)
    withTable("amt_lmc_ict") {
      val name = "amt_lmc_ict"
      createAMTTable(name, checkpointInterval = 100)
      sql(s"INSERT INTO $name VALUES (1)") // trailing delta carrying both ICT and (injected) LMC.

      val deltaLog = deltaLogForName(name)
      val version = deltaLog.unsafeVolatileSnapshot.version
      // The true in-commit-timestamp before injection; both injectors preserve it.
      val expectedIct = deltaLog.unsafeVolatileSnapshot.getInCommitTimestampOpt.getOrElse {
        fail("Expected a non-None in-commit-timestamp.")
      }
      injectLmc(deltaLog, version, lmc = Some(lmc))

      val snapshot = freshSnapshotAt(name, version)
      assert(snapshot.lastManifestCommitOpt.contains(lmc),
        "LMC must survive on a row that also carries an in-commit-timestamp.")
      assert(snapshot.getInCommitTimestampOpt.contains(expectedIct),
        "The in-commit-timestamp must still be resolved from the same row.")
    }
  }

  test("lastManifestCommitOpt is None when no reference is recorded") {
    withTable("amt_lmc_none") {
      val name = "amt_lmc_none"
      createAMTTable(name, checkpointInterval = 100)
      sql(s"INSERT INTO $name VALUES (1)")
      // No manifest-commit reference is recorded on either the CommitInfo or the CRC.
      assert(freshSnapshotAt(name, 1).lastManifestCommitOpt.isEmpty)
    }
  }
}

/** Runs the shared cases with `.crc` files enabled; the reference is seeded to CommitInfo + CRC. */
class SnapshotLastManifestCommitSuite extends SnapshotLastManifestCommitSuiteBase {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, "true")

  override protected def injectLmc(
      deltaLog: DeltaLog, version: Long, lmc: Option[LastManifestCommit]): Unit = {
    injectLmcIntoCommitInfo(deltaLog, version, lmc)
    injectLmcIntoCrc(deltaLog, version, lmc)
  }
}

/** Runs the shared cases with `.crc` files disabled; the reference is seeded to CommitInfo only. */
class SnapshotLastManifestCommitWithoutCRCSuite extends SnapshotLastManifestCommitSuiteBase {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, "false")

  override protected def injectLmc(
      deltaLog: DeltaLog, version: Long, lmc: Option[LastManifestCommit]): Unit = {
    injectLmcIntoCommitInfo(deltaLog, version, lmc)
  }

  test("lastManifestCommitOpt falls back to CommitInfo when no other source carries the ref") {
    // With CRC unavailable, when the snapshot sits on an AMT checkpoint, there is no trailing delta
    // to provide the CommitInfo during P&M query, so we must fallback to a direct CommitInfo read.
    val lmc = LastManifestCommit(version = 7, contentRootVersion = 5)
    withTable("amt_lmc_fallback") {
      val name = "amt_lmc_fallback"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)")
      sql(s"INSERT INTO $name VALUES (2)") // v2: emit; carries the inline checkpoint action.

      val deltaLog = deltaLogForName(name)
      injectLmc(deltaLog, version = 2, lmc = Some(lmc))

      // Build the AMT provider from v2's emitted checkpoint action and stub it into a fresh
      // snapshot's log segment, trimming the version's delta as cold discovery eventually will.
      val checkpoint = checkpointsAt(deltaLog, 2).headOption.getOrElse {
        fail("v2 must emit an inline AMT checkpoint action.")
      }
      val provider = AMTCheckpointProvider.fromCheckpoint(spark, deltaLog, checkpoint)
      val coldSnapshot = freshSnapshotAt(name, 2)
      val segment = coldSnapshot.logSegment.copy(checkpointProvider = provider, deltas = Nil)
      val snapshot = new Snapshot(
        path = coldSnapshot.path,
        version = coldSnapshot.version,
        logSegment = segment,
        deltaLog = coldSnapshot.deltaLog,
        checksumOpt = None // No CRC, so reconstruction has no source other than the CommitInfo.
      )

      assert(amtProvider(snapshot).isDefined, "Snapshot must carry the (stubbed) AMT provider.")
      assert(snapshot.logSegment.deltas.isEmpty, "Segment must have no trailing deltas.")
      assert(snapshot.checksumOpt.isEmpty, "Snapshot must have no CRC.")

      implicit val usageLogs: Seq[UsageRecord] = Log4jUsageLogger.track {
        assert(snapshot.lastManifestCommitOpt.contains(lmc),
          "The CommitInfo fallback must resolve the reference.")
      }
      assertUsageLog(AMTUsageLogs.LAST_MANIFEST_COMMIT_READ_FROM_COMMIT_INFO, isPresent = true)
    }
  }
}
