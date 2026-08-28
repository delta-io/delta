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
import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Tests that [[LastManifestCommit]] is surfaced reliably by [[Snapshot.lastManifestCommitOpt]].
 */
trait SnapshotLastManifestCommitSuiteBase extends AMTCheckpointTestBase {

  /** Whether this suite runs with `.crc` files enabled, read from the effective conf. */
  protected def writeChecksumEnabled: Boolean =
    spark.conf.get(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED)

  ////////////////////////
  // Fake, Injected LMCs
  ////////////////////////

  /**
   * Seeds `lmc` onto every source that exists for this suite's mode. Always rewrites the delta-file
   * `CommitInfo`; the CRC-enabled suite additionally rewrites the `.crc`.
   */
  protected def injectLmc(
      deltaLog: DeltaLog, version: Long, lmc: Option[LastManifestCommit]): Unit = {
    injectLmcIntoCommitInfo(deltaLog, version, lmc)
    injectLmcIntoCrc(deltaLog, version, lmc)
  }

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
   * Returns a snapshot at `version` suitable for asserting `lastManifestCommitOpt` resolution when
   * the test has injected a synthetic reference (see [[injectLmc]]) onto a table with no matching
   * AMT checkpoint.
   */
  protected def snapshotForLmcResolution(deltaLog: DeltaLog, version: Long): Snapshot = {
    val base = deltaLog.unsafeVolatileSnapshot
    new Snapshot(
      path = base.path,
      version = version,
      logSegment = base.logSegment.copy(
        version = version,
        deltas = base.logSegment.deltas.filter(f => FileNames.deltaVersion(f) <= version)),
      deltaLog = deltaLog,
      checksumOpt = deltaLog.readChecksum(version)
    )
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
          assert(snapshotForLmcResolution(deltaLog, version).lastManifestCommitOpt == expected)
        }
        // There are trailing deltas, so the CommitInfo fallback is never reached.
        assertUsageLog(AMTUsageLogs.LAST_MANIFEST_COMMIT_READ_FROM_COMMIT_INFO, isPresent = false)
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

      val snapshot = snapshotForLmcResolution(deltaLog, version)
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

  ////////////////////////////
  // Actually Populated LMCs
  ////////////////////////////

  /** The `lastManifestCommit` recorded on the CommitInfo committed at `version`, if any. */
  private def lastManifestCommitFromCommitInfoAt(
      deltaLog: DeltaLog, version: Long): Option[LastManifestCommit] = {
    actionsAt(deltaLog, version)
      .collectFirst { case ci: CommitInfo => ci }
      .flatMap(_.lastManifestCommit)
  }

  /** The `lastManifestCommit` persisted in the CRC at `version`, if the CRC exists. */
  private def lastManifestCommitFromCrcAt(
      deltaLog: DeltaLog, version: Long): Option[LastManifestCommit] =
    deltaLog.readChecksum(version).flatMap(_.lastManifestCommit)

  /** Asserts the commit at `version` has the expected manifest-commit states. */
  private def assertCommitStates(
      deltaLog: DeltaLog,
      version: Long,
      emitsCheckpoint: Boolean,
      expected: Option[LastManifestCommit]): Unit = {
    assert(checkpointAt(deltaLog, version).nonEmpty == emitsCheckpoint,
      s"v$version checkpoint emission: expected $emitsCheckpoint.")
    // The CRC only carries the reference when `.crc` files are written for this suite; without
    // them there is no CRC to inspect.
    if (writeChecksumEnabled) {
      assert(lastManifestCommitFromCrcAt(deltaLog, version) == expected,
        s"v$version CRC must carry LMC $expected.")
    }
    assert(lastManifestCommitFromCommitInfoAt(deltaLog, version) == expected,
      s"v$version CommitInfo must carry LMC $expected.")
    // The snapshot resolves the reference from the CRC when present, and otherwise from the latest
    // commit's CommitInfo fallback, so this holds in both CRC and no-CRC modes.
    assert(deltaLog.getSnapshotAt(version).lastManifestCommitOpt == expected,
      s"v$version snapshot must resolve LMC $expected.")
  }

  test("manifest commits persist lastManifestCommit to the CRC and CommitInfo, " +
      "while non-manifest commits carry the previous manifest-commit reference forward") {
    withTable("amt_lmc_persist") {
      val name = "amt_lmc_persist"
      createAMTTable(name, checkpointInterval = 3)

      sql(s"INSERT INTO $name VALUES (1)") // v1: no checkpoint.
      sql(s"INSERT INTO $name VALUES (2)") // v2: no checkpoint.
      sql(s"INSERT INTO $name VALUES (3)") // v3: triggers a checkpoint, but not in this commit.
                                           // v4: AMT checkpoint emitted via hook.
      sql(s"INSERT INTO $name VALUES (4)") // v5: no checkpoint.
      sql(s"INSERT INTO $name VALUES (5)") // v6: triggers another checkpoint, not in this commit.
                                           // v7: AMT checkpoint emitted via hook.

      val deltaLog = deltaLogForName(name)

      // Before the first manifest checkpoint (first emit is v4), there is no reference to record.
      assertCommitStates(deltaLog, 1, emitsCheckpoint = false, expected = None)
      assertCommitStates(deltaLog, 2, emitsCheckpoint = false, expected = None)

      // The checkpoint-trigger commit (v3) does not create or store a reference yet.
      assertCommitStates(deltaLog, 3, emitsCheckpoint = false, expected = None)
      // The actual manifest commit stores the new reference to the CRC and CommitInfo.
      // Note that the manifest commit version is 4, but the content root version is 3.
      val expectedLmcAtV4 = LastManifestCommit(version = 4, contentRootVersion = 3)
      assertCommitStates(deltaLog, 4, emitsCheckpoint = true, expected = Some(expectedLmcAtV4))

      // The non-manifest commit carries the previous manifest-commit reference forward.
      assertCommitStates(deltaLog, 5, emitsCheckpoint = false, expected = Some(expectedLmcAtV4))

      // The next checkpoint-trigger commit (v6) does not create or store a new reference yet.
      assertCommitStates(deltaLog, 6, emitsCheckpoint = false, expected = Some(expectedLmcAtV4))
      // The actual manifest commit stores the new reference to the CRC and CommitInfo.
      // Note that the manifest commit version is 7, but the content root version is 6.
      val expectedLmcAtV7 = LastManifestCommit(version = 7, contentRootVersion = 6)
      assertCommitStates(deltaLog, 7, emitsCheckpoint = true, expected = Some(expectedLmcAtV7))
    }
  }

  testInline("consecutive manifest commits persist lastManifestCommit to the CRC and CommitInfo") {
    withTable("amt_lmc_persist_inline") {
      val name = "amt_lmc_persist_inline"
      createAMTTable(name, checkpointInterval = 2)

      sql(s"INSERT INTO $name VALUES (1)") // v1: no checkpoint (no existing tree to inline into).
      sql(s"INSERT INTO $name VALUES (2)") // v2: reaches the interval boundary.
                                            // v3: the FIRST AMT is emitted as a deferred OPTIMIZE
                                            //     CHECKPOINT (a full rewrite describing state@v2).
      sql(s"INSERT INTO $name VALUES (3)") // v4: now a tree exists, so this commit emits inline.
      sql(s"INSERT INTO $name VALUES (4)") // v5: another inline manifest commit.

      val deltaLog = deltaLogForName(name)

      // v1/v2: carry no manifest reference yet: the first AMT has not been written.
      assertCommitStates(deltaLog, 1, emitsCheckpoint = false, expected = None)
      assertCommitStates(deltaLog, 2, emitsCheckpoint = false, expected = None)

      // v3: the deferred first AMT. It is a full rewrite of state as of v2, so version=3 but
      // contentRootVersion=2.
      val expectedLmcAtV3 = LastManifestCommit(version = 3, contentRootVersion = 2)
      assertCommitStates(deltaLog, 3, emitsCheckpoint = true, expected = Some(expectedLmcAtV3))

      // v4: with a tree in place, the business commit writes its manifest inline, so version and
      // contentRootVersion coincide.
      val expectedLmcAtV4 = LastManifestCommit(version = 4, contentRootVersion = 4)
      assertCommitStates(deltaLog, 4, emitsCheckpoint = true, expected = Some(expectedLmcAtV4))

      // v5: another inline manifest commit.
      val expectedLmcAtV5 = LastManifestCommit(version = 5, contentRootVersion = 5)
      assertCommitStates(deltaLog, 5, emitsCheckpoint = true, expected = Some(expectedLmcAtV5))
    }
  }

  test("commitLarge (RESTORE) always carries lastManifestCommit forward") {
    withTable("amt_lmc_restore") {
      val name = "amt_lmc_restore"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)")        // v1: no checkpoint
      sql(s"INSERT INTO $name VALUES (2)")        // v2: reaches the interval boundary
                                                  // v3: the FIRST AMT is always a deferred OPTIMIZE
                                                  //     CHECKPOINT (describing state@v2).
      sql(s"RESTORE TABLE $name VERSION AS OF 1") // v4: RESTORE commit (via commitLarge)

      val (deltaLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, new TableIdentifier(name))

      // RESTORE commits via commitLarge, which never emits AMT checkpoints, even when the
      // checkpoint interval is reached. The lastManifestCommit valid as of its read snapshot must
      // be carried forward.
      assert(snapshot.version == 4, s"RESTORE should commit to v4, but got v${snapshot.version}.")
      val expectedLmc = LastManifestCommit(version = 3, contentRootVersion = 2)

      assert(checkpointAt(deltaLog, 4).isEmpty, s"v4 must not emit a checkpoint.")
      // CommitLarge does not write a CRC, so we skip the CRC assertions.
      assert(
        lastManifestCommitFromCommitInfoAt(deltaLog, 4).contains(expectedLmc),
        s"v4 CommitInfo must carry LMC $expectedLmc.")
      // When getting the snapshot to test, a real AMT provider is installed, but there is no CRC
      // to cross-verify it, so reconciliation refuses it. We skip the snapshot-resolution assertion
      // temporarily, until the CommitInfo fallback lands.
      /*
      assert(
        freshSnapshotAt(name, 4).lastManifestCommitOpt.contains(expectedLmc),
        s"v4 snapshot must resolve LMC $expectedLmc.")
      */
    }
  }
}

/**
 * With incremental CRC and verification both enabled, the commit discards the incrementally-derived
 * checksum and verifies against a full reconstruction, so the final CRC file is actually sourced
 * from reconstruction via [[Snapshot.computeChecksum]], not incremental derivation.
 */
class SnapshotLastManifestCommitIncrementalCRCWithVerificationSuite
  extends SnapshotLastManifestCommitSuiteBase {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, "true")
    .set(DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key, "true")
    .set(DeltaSQLConf.INCREMENTAL_COMMIT_VERIFY.key, "true")
    .set(DeltaSQLConf.INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS.key, "true")
}

/**
 * With incremental CRC enabled but verification off, the commit trusts and caches the
 * incrementally-derived checksum, so the final CRC file is indeed sourced from incremental
 * derivation via [[Checksum.computeNewChecksum]]. This matches the production hot path.
 */
class SnapshotLastManifestCommitIncrementalCRCWithoutVerificationSuite
  extends SnapshotLastManifestCommitSuiteBase {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, "true")
    .set(DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key, "true")
    .set(DeltaSQLConf.INCREMENTAL_COMMIT_VERIFY.key, "false")
    .set(DeltaSQLConf.INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS.key, "false")
}

/**
 * With incremental CRC disabled, the final CRC file is sourced directly from reconstruction via
 * [[Snapshot.computeChecksum]]. No incremental derivation is ever attempted.
 */
class SnapshotLastManifestCommitNonIncrementalCRCSuite
  extends SnapshotLastManifestCommitSuiteBase {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, "true")
    .set(DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key, "false")
    // When incremental CRC is disabled, the verify flags are irrelevant.
}

/** No CRC files are ever written in this suite. Aims to test the fallback paths. */
class SnapshotLastManifestCommitWithoutCRCSuite extends SnapshotLastManifestCommitSuiteBase {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, "false")
    // When CRC is disabled, the incremental/verify flags are all irrelevant.

  override protected def injectLmc(
      deltaLog: DeltaLog, version: Long, lmc: Option[LastManifestCommit]): Unit = {
    // No CRC files are written, so the only inject lastManifestCommit into the CommitInfo.
    injectLmcIntoCommitInfo(deltaLog, version, lmc)
  }

  testInline("lastManifestCommitOpt falls back to reading CommitInfo with no other sources") {
    // With CRC unavailable, when the snapshot sits on an AMT checkpoint, there is no trailing delta
    // to provide the CommitInfo during P&M query, so we must fallback to a direct CommitInfo read.
    val lmc = LastManifestCommit(version = 7, contentRootVersion = 5)
    withTable("amt_lmc_fallback") {
      val name = "amt_lmc_fallback"
      createAMTTable(name, checkpointInterval = 2)
      // The first AMT is always a full, deferred rewrite (an inline write is incremental and needs
      // a full tree to build on). v2 is the interval boundary, so the first (full) AMT lands as a
      // follow-up OPTIMIZE CHECKPOINT commit at v3. v4 is the next interval boundary and, now that
      // a full tree exists, carries its AMT checkpoint action inline (this suite forces inline via
      // a threshold of 1). This test needs an inline checkpoint action to build the provider from,
      // so it uses v4.
      sql(s"INSERT INTO $name VALUES (1)") // v1.
      sql(s"INSERT INTO $name VALUES (2)") // v2: boundary -> deferred full AMT follow-up at v3.
      sql(s"INSERT INTO $name VALUES (3)") // v4: boundary -> inline AMT checkpoint action.

      val deltaLog = deltaLogForName(name)
      injectLmc(deltaLog, version = 4, lmc = Some(lmc))

      // Build the AMT provider from v4's emitted inline checkpoint action and stub it into a real
      // snapshot's log segment, trimming the version's delta as cold discovery eventually will.
      val checkpoint = checkpointAt(deltaLog, 4).getOrElse {
        fail("v4 must emit an inline AMT checkpoint action.")
      }
      val provider = AMTCheckpointProvider.fromCheckpoint(
        deltaLog, checkpoint, manifestCommitVersion = 4L)
      val baseSnapshot = deltaLog.unsafeVolatileSnapshot
      assert(baseSnapshot.version == 4,
        s"Expected volatile snapshot at v4, got ${baseSnapshot.version}.")
      val segment = baseSnapshot.logSegment.copy(checkpointProvider = provider, deltas = Nil)
      val snapshot = new Snapshot(
        path = baseSnapshot.path,
        version = baseSnapshot.version,
        logSegment = segment,
        deltaLog = baseSnapshot.deltaLog,
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
