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

import org.apache.spark.sql.delta.{DeltaLog, DeltaMinorCompactionTestUtils, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, CommitInfo, LastManifestCommit}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, FileNames, JsonUtils}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.metric.SQLMetrics

class AMTSnapshotDiscoverySuite
  extends AMTCheckpointTestBase
  with DeltaMinorCompactionTestUtils {

  /** Whether this suite runs with `.crc` files enabled, read from the effective conf. */
  protected def writeChecksumEnabled: Boolean =
    spark.conf.get(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED)

  test("[snapshot init] rejects an AMT checkpoint without deltaAtCheckpointVersionOpt") {
    val name = "amt_missing_delta_at_checkpoint"
    withTable(name) {
      createAMTTable(name, checkpointInterval = 2)
      (1 to 2).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
      val snapshot = deltaLogForName(name).update()
      assert(amtProvider(snapshot).isDefined)
      assert(snapshot.logSegment.deltaAtCheckpointVersionOpt.isDefined)
      // Reuse the real checkpoint and remove only the required commit-file reference.
      val incompleteSegment = snapshot.logSegment.copy(deltaAtCheckpointVersionOpt = None)
      val error = intercept[IllegalStateException] {
        new Snapshot(
          path = snapshot.path,
          version = snapshot.version,
          logSegment = incompleteSegment,
          deltaLog = snapshot.deltaLog,
          checksumOpt = snapshot.checksumOpt
        )
      }
      assert(error.getMessage.startsWith(
        "An AMT-enabled snapshot must define deltaAtCheckpointVersionOpt, got None."))
    }
  }

  ////////////////////////////
  // Cold snapshot discovery
  ////////////////////////////

  /** Loads a genuinely cold deltaLog + snapshot (cache cleared) for the given table. */
  protected def coldLoad(tableName: String): (DeltaLog, Snapshot) = {
    DeltaLog.clearCache()
    DeltaLog.forTableWithSnapshot(spark, new TableIdentifier(tableName))
  }

  /** Asserts that the cold snapshot is installed with the expected state. */
  private def assertColdSnapshotStates(
      tableName: String,
      version: Long,
      amtCheckpointVersion: Option[Long],
      trailingDeltas: Seq[Long],
      lastManifestCommit: Option[LastManifestCommit]): Unit = {
    val (_, snapshot) = coldLoad(tableName)
    assert(snapshot.version == version)
    assert(snapshot.logSegment.version == version)
    assert(snapshot.logSegment.deltas.map(FileNames.deltaVersion) == trailingDeltas)
    assert(snapshot.lastManifestCommitOpt == lastManifestCommit)
    assert(amtProvider(snapshot).map(_.version) == amtCheckpointVersion)
    // The AMT checkpoint is trusted once the manifest-commit reference corroborates it. With a CRC
    // that reference comes from the checksum; without one it is recovered from the latest commit's
    // CommitInfo, and no checksum is resolved.
    if (writeChecksumEnabled) {
      val checksum = snapshot.checksumOpt.getOrElse(
        fail(s"v$version: a cold AMT read with CRC enabled must resolve a checksum."))
      assert(checksum.lastManifestCommit == lastManifestCommit,
        s"v$version: the CRC's manifest-commit reference must match the snapshot's.")
    } else {
      assert(snapshot.checksumOpt.isEmpty,
        s"v$version: no checksum should be resolved when CRC writes are disabled.")
    }
  }

  testInline("[cold init] installs the correct provider from up-to-date _last_checkpoint: inline") {
    withTable("amt_cold_discovery_inline") {
      val name = "amt_cold_discovery_inline"
      createAMTTable(name, checkpointInterval = 2)
      // Inline mode, interval 2. Inline-incremental extends an existing manifest tree, so it cannot
      // bootstrap the first one: the first (full) AMT is always a deferred OPTIMIZE CHECKPOINT.
      // Only once that tree exists does each subsequent business commit write its manifest inline.
      //   v0: CREATE                           (genesis, no manifest)
      //   v1: INSERT #1                        (no full AMT yet -> cannot inline)
      //   v2: INSERT #2 (reaches boundary)     (still cannot inline)
      //   v3: OPTIMIZE CHECKPOINT (state@v2)   (the first, full AMT; deferred)
      //   v4: INSERT #3 + inline AMT (state@v4)
      //   v5: INSERT #4 + inline AMT (state@v5)

      // v0: table just created.
      assertColdSnapshotStates(
        tableName = name,
        version = 0,
        amtCheckpointVersion = None,
        trailingDeltas = Seq(0L),
        lastManifestCommit = None)

      // v1: no full AMT exists yet, so this commit cannot inline one.
      sql(s"INSERT INTO $name VALUES (1)")
      assert(rootFiles(tablePath(name)).isEmpty)
      assert(leafFiles(tablePath(name)).isEmpty)
      assertColdSnapshotStates(
        tableName = name,
        version = 1,
        amtCheckpointVersion = None,
        trailingDeltas = Seq(0L, 1L),
        lastManifestCommit = None)

      // v2 reaches the interval boundary, but the first AMT can never ride inline: it lands as the
      // deferred follow-up OPTIMIZE CHECKPOINT at v3, describing state as of v2.
      sql(s"INSERT INTO $name VALUES (2)")
      val lmcAtV3 = LastManifestCommit(version = 3, contentRootVersion = 2)
      assertColdSnapshotStates(
        tableName = name,
        version = 3,
        amtCheckpointVersion = Some(2),
        trailingDeltas = Seq(3L),
        lastManifestCommit = Some(lmcAtV3))

      // v4: a manifest tree now exists, so this business commit writes its AMT inline. Version and
      // content-root version coincide, and the segment trims to no trailing deltas.
      sql(s"INSERT INTO $name VALUES (3)")
      val lmcAtV4 = LastManifestCommit(version = 4, contentRootVersion = 4)
      assertColdSnapshotStates(
        tableName = name,
        version = 4,
        amtCheckpointVersion = Some(4),
        trailingDeltas = Seq.empty,
        lastManifestCommit = Some(lmcAtV4))

      // v5: each subsequent commit keeps inlining its own manifest.
      sql(s"INSERT INTO $name VALUES (4)")
      val lmcAtV5 = LastManifestCommit(version = 5, contentRootVersion = 5)
      assertColdSnapshotStates(
        tableName = name,
        version = 5,
        amtCheckpointVersion = Some(5),
        trailingDeltas = Seq.empty,
        lastManifestCommit = Some(lmcAtV5))
    }
  }

  test("[cold init] installs the correct provider from up-to-date _last_checkpoint: deferred") {
    withTable("amt_cold_discovery_deferred") {
      val name = "amt_cold_discovery_deferred"
      createAMTTable(name, checkpointInterval = 3)
      // Deferred mode, interval 3. A business INSERT that reaches a boundary is followed
      // synchronously by an OPTIMIZE CHECKPOINT commit landing one version later.
      //   v0: CREATE
      //   v1: INSERT #1                        (before the first checkpoint)
      //   v2: INSERT #2                        (before the first checkpoint)
      //   v3: INSERT #3 (reaches boundary)
      //   v4: OPTIMIZE CHECKPOINT (state@v3)
      //   v5: INSERT #4                        (non-manifest, carries the v3 reference forward)
      //   v6: INSERT #5 (reaches next boundary)
      //   v7: OPTIMIZE CHECKPOINT (state@v6)

      // v0: table just created.
      assertColdSnapshotStates(
        tableName = name,
        version = 0,
        amtCheckpointVersion = None,
        trailingDeltas = Seq(0L),
        lastManifestCommit = None)

      // v1: before the first checkpoint. No provider, no reference yet.
      sql(s"INSERT INTO $name VALUES (1)")
      assertColdSnapshotStates(
        tableName = name,
        version = 1,
        amtCheckpointVersion = None,
        trailingDeltas = Seq(0L, 1L),
        lastManifestCommit = None)

      // v2: still before the first checkpoint.
      sql(s"INSERT INTO $name VALUES (2)")
      assertColdSnapshotStates(
        tableName = name,
        version = 2,
        amtCheckpointVersion = None,
        trailingDeltas = Seq(0L, 1L, 2L),
        lastManifestCommit = None)

      // v3 INSERT reaches the boundary; the follow-up OPTIMIZE CHECKPOINT lands at v4 describing
      // state as of v3. Discovery installs the provider at v3 and trims to the checkpoint commit.
      sql(s"INSERT INTO $name VALUES (3)")
      val lmcAtV4 = LastManifestCommit(version = 4, contentRootVersion = 3)
      assertColdSnapshotStates(
        tableName = name,
        version = 4,
        amtCheckpointVersion = Some(3),
        trailingDeltas = Seq(4L),
        lastManifestCommit = Some(lmcAtV4))

      // v5: a non-manifest INSERT. The v3 checkpoint stays latest, the segment carries the v4
      // checkpoint commit plus the v5 delta, and the reference is carried forward.
      sql(s"INSERT INTO $name VALUES (4)")
      assertColdSnapshotStates(
        tableName = name,
        version = 5,
        amtCheckpointVersion = Some(3),
        trailingDeltas = Seq(4L, 5L),
        lastManifestCommit = Some(lmcAtV4))

      // v6 INSERT reaches the next boundary; the follow-up OPTIMIZE CHECKPOINT lands at v7
      // describing state as of v6. Discovery installs the provider at v6 and trims accordingly.
      sql(s"INSERT INTO $name VALUES (5)")
      val lmcAtV7 = LastManifestCommit(version = 7, contentRootVersion = 6)
      assertColdSnapshotStates(
        tableName = name,
        version = 7,
        amtCheckpointVersion = Some(6),
        trailingDeltas = Seq(7L),
        lastManifestCommit = Some(lmcAtV7))
    }
  }

  /** The `_last_checkpoint` file system and path for a table accessed by name. */
  private def lastCheckpointFsAndPath(tableName: String): (FileSystem, Path) = {
    val log = deltaLogForName(tableName)
    val path = log.LAST_CHECKPOINT
    (path.getFileSystem(log.newDeltaHadoopConf()), path)
  }

  /** The raw bytes of the current `_last_checkpoint` file. */
  private def readLastCheckpointBytes(tableName: String): Array[Byte] = {
    val (fs, path) = lastCheckpointFsAndPath(tableName)
    val in = fs.open(path)
    try IOUtils.toByteArray(in) finally in.close()
  }

  /** Overwrites `_last_checkpoint` with `bytes` (used to plant a stale hint). */
  private def overwriteLastCheckpoint(tableName: String, bytes: Array[Byte]): Unit = {
    val (fs, path) = lastCheckpointFsAndPath(tableName)
    val out = fs.create(path, true)
    try out.write(bytes) finally out.close()
  }

  /** Deletes the `_last_checkpoint` file (used to simulate a missing hint). */
  private def deleteLastCheckpoint(tableName: String): Unit = {
    val (fs, path) = lastCheckpointFsAndPath(tableName)
    assert(fs.delete(path, false), s"failed to delete $path")
  }

  /** Replaces only the manifest reference in a commit's CommitInfo. */
  protected def overwriteCommitInfoLastManifestCommit(
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      version: Long,
      lastManifestCommitOpt: Option[LastManifestCommit]): Unit = {
    val commitPath = DeltaCommitFileProvider(snapshot).deltaFile(version)
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val actions = deltaLog.store.read(commitPath, hadoopConf).map(Action.fromJson)
    assert(actions.exists(_.isInstanceOf[CommitInfo]))
    val rewritten = actions.map {
      case ci: CommitInfo => ci.copy(lastManifestCommit = lastManifestCommitOpt).json
      case action => action.json
    }
    deltaLog.store.write(commitPath, rewritten.iterator, overwrite = true, hadoopConf)
  }

  test("[cold init] updates to the correct provider when _last_checkpoint is stale") {
    val name = "amt_stale_last_checkpoint"
    withTable(name) {
      createAMTTable(name, checkpointInterval = 2)
      // Reach the first deferred checkpoint (content root 2, recorded at v3) and capture its hint.
      (1 to 2).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
      assert(deltaLogForName(name).unsafeVolatileSnapshot.version == 3,
        "The first deferred AMT must land at v3.")
      val staleHint = readLastCheckpointBytes(name)

      // Advance to the second deferred checkpoint (content root 4, recorded at v5).
      sql(s"INSERT INTO $name VALUES (3)")
      assert(deltaLogForName(name).unsafeVolatileSnapshot.version == 5,
        "The second deferred AMT must land at v5.")

      // Plant the stale hint: it points to content root 2 while the table is at content root 4.
      overwriteLastCheckpoint(name, staleHint)
      val lmcAtV5 = LastManifestCommit(version = 5, contentRootVersion = 4)
      assertColdSnapshotStates(
        tableName = name,
        version = 5,
        amtCheckpointVersion = Some(4),
        trailingDeltas = Seq(5L),
        lastManifestCommit = Some(lmcAtV5))
    }
  }

  testAcrossAMTCheckpointScenarios(
      "[cold init] rejects an AMT provider ahead of the last manifest reference",
      "amt_ahead_reference")(
      setup = name => sql(s"INSERT INTO $name VALUES (1)"),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (2)"))) { context =>
    val deltaLog = context.postCheckpointSnapshot.deltaLog
    val version = context.manifestCommitVersion
    val staleReference = context.postSetupSnapshot.lastManifestCommitOpt.get
    assert(context.provider.version > staleReference.contentRootVersion)
    // Read the recording version itself: M == V must not receive the time-travel exemption.
    assert(context.postCheckpointSnapshot.version == version)
    if (writeChecksumEnabled) {
      val checksum = deltaLog.readChecksum(version).get
      val corrupted = checksum.copy(lastManifestCommit = Some(staleReference))
      deltaLog.store.write(
        FileNames.checksumFile(deltaLog.logPath, version),
        Iterator(JsonUtils.toJson(corrupted)),
        overwrite = true,
        deltaLog.newDeltaHadoopConf())
    } else {
      // The post-commit snapshot can retain a staged path after backfill. Resolve the file through
      // a cold snapshot so we overwrite the same copy that the subsequent cold read will use.
      val (coldLog, coldSnapshot) = coldLoad(context.tableName)
      overwriteCommitInfoLastManifestCommit(coldLog, coldSnapshot, version, Some(staleReference))
    }

    val e = intercept[IllegalStateException](coldLoad(context.tableName))
    assert(e.getMessage.contains("AMT checkpoint mismatch"), e.getMessage)
  }

  test("[cold init] builds the correct provider when _last_checkpoint is absent") {
    withTable("amt_absent_last_checkpoint") {
      val name = "amt_absent_last_checkpoint"
      createAMTTable(name, checkpointInterval = 2)
      // Same timeline as the deferred cold test: 2 INSERTs land the v2 tree, recorded at v3.
      (1 to 2).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
      val lmcAtV3 = LastManifestCommit(version = 3, contentRootVersion = 2)

      // Baseline: the fresh cold read resolves the v2 tree via the hint.
      assertColdSnapshotStates(
        tableName = name,
        version = 3,
        amtCheckpointVersion = Some(2),
        trailingDeltas = Seq(3L),
        lastManifestCommit = Some(lmcAtV3))

      // Delete the hint; the cold read must resolve the same state, rebuilt from the CRC.
      deleteLastCheckpoint(name)
      assertColdSnapshotStates(
        tableName = name,
        version = 3,
        amtCheckpointVersion = Some(2),
        trailingDeltas = Seq(3L),
        lastManifestCommit = Some(lmcAtV3))
    }
  }

  ///////////////////////////
  // deltaLog.update()
  ///////////////////////////

  /** The catalog table for a catalog-managed AMT table accessed by name. */
  private def catalogTableFor(tableName: String): CatalogTable =
    spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))

  /** The delta versions of the deltas kept in the snapshot's log segment. */
  private def segmentDeltaVersions(snapshot: Snapshot): Seq[Long] =
    snapshot.logSegment.deltas.map(FileNames.deltaVersion)

  /**
   * The master equivalence oracle for the warm-update path. It checks two independent things:
   *
   *  1. Absolute expectations on the warm snapshot (version, AMT provider checkpoint version,
   *     trailing delta versions). These do NOT use cold as the oracle, so a discovery bug that
   *     corrupts BOTH the warm and the cold path identically is still caught here.
   *  2. That the warm snapshot is otherwise indistinguishable from a fresh cold load: structural
   *     fields, the manifest-commit reference, the installed CRC's file count, protocol/metadata,
   *     and -- the strongest check -- that state reconstructed from each yields identical contents.
   */
  private def assertWarmMatchesCold(
      warmSnapshot: Snapshot,
      tableName: String,
      expectedVersion: Int,
      expectedProviderVersion: Option[Int],
      expectedTrailingDeltas: Seq[Int]): Unit = {
    // (1) Absolute expectations -- pinned literals, independent of the cold path.
    assert(warmSnapshot.version == expectedVersion,
      s"warm v${warmSnapshot.version} != expected v$expectedVersion.")
    assert(amtProvider(warmSnapshot).map(_.checkpointVersion) == expectedProviderVersion,
      s"warm provider ${amtProvider(warmSnapshot).map(_.checkpointVersion)} != " +
        s"expected $expectedProviderVersion.")
    assert(segmentDeltaVersions(warmSnapshot) == expectedTrailingDeltas,
      s"warm deltas ${segmentDeltaVersions(warmSnapshot)} != expected $expectedTrailingDeltas.")

    // (2) Warm must additionally match a fresh cold load in every observable field.
    val (coldDeltaLog, coldSnapshot) = coldLoad(tableName)
    assert(warmSnapshot.version == coldSnapshot.version,
      s"warm v${warmSnapshot.version} != cold v${coldSnapshot.version}.")
    assert(warmSnapshot.logSegment.equals(coldSnapshot.logSegment),
      "log segment must be equal between warm and cold.")
    assert(warmSnapshot.lastManifestCommitOpt == coldSnapshot.lastManifestCommitOpt,
      s"warm ${warmSnapshot.lastManifestCommitOpt} != cold ${coldSnapshot.lastManifestCommitOpt}.")
    if (writeChecksumEnabled) {
      assert(warmSnapshot.checksumOpt == coldSnapshot.checksumOpt,
        "checksum must be equal between warm and cold.")
    }
    assert(warmSnapshot.protocol == coldSnapshot.protocol,
      "protocol must be equal between warm and cold.")
    assert(warmSnapshot.metadata == coldSnapshot.metadata,
      "metadata must be equal between warm and cold.")
    // Data-level: reconstructed contents must match, which catches a wrong log segment even when
    // the scalar fields above happen to agree.
    checkAnswer(
      warmSnapshot.deltaLog.createDataFrame(
        warmSnapshot, warmSnapshot.allFilesViaStateReconstruction.collect().toSeq),
      coldDeltaLog.createDataFrame(
        coldSnapshot, coldSnapshot.allFilesViaStateReconstruction.collect().toSeq).collect().toSeq)
  }

  /**
   * Context for the warm update test harness.
   *
   * @param label The test name.
   * @param staleVersion The version of the stale DeltaLog handle.
   * @param cpProviderVersions The versions of all the checkpoint providers in the table.
   * @param expectedVersion The expected snapshot version after update().
   * @param expectedCpVersion The expected checkpoint provider version installed by update().
   * @param expectedTrailingDeltas The expected trailing deltas installed by update().
   */
  private case class WarmUpdateContext(
      label: String,
      staleVersion: Int,
      cpProviderVersions: Seq[Int],
      latestCommitVersion: Int,
      expectedCpVersion: Option[Int],
      expectedTrailingDeltas: Seq[Int]) {
    // For simplicity in versioning, the test harness always uses inline manifest commits. But since
    // the first checkpoint cannot be inline, we trigger a full checkpoint at v1, defer its emission
    // to v2, and bump up the checkpoint interval to avoid emitting more deferred checkpoints in v3.
    // The test harness then starts from v3.
    require(staleVersion >= 3)
    require(cpProviderVersions.head == 1)
    require(staleVersion <= latestCommitVersion)
    require(cpProviderVersions.forall(_ <= latestCommitVersion))
    require(expectedCpVersion.forall(_ <= latestCommitVersion))
    require(expectedTrailingDeltas.forall(_ <= latestCommitVersion))
  }

  private def runWarmUpdateContext(ctx: WarmUpdateContext): Unit = {
    val name = s"amt_warm_matrix"
    withTable(name) {
      createAMTTable(name, checkpointInterval = 1)
      sql(s"INSERT INTO $name VALUES (1)")  // v1: triggers a deferred checkpoint
                                            // v2: OPTIMIZE CHECKPOINT (state@v1)
      sql(s"ALTER TABLE $name SET TBLPROPERTIES ('delta.checkpointInterval' = '1000')")
                                            // v3: bump up the interval for maneuverability
      assert(deltaLogForName(name).unsafeVolatileSnapshot.version == 3)

      (4 to ctx.staleVersion).foreach { i =>
        if (ctx.cpProviderVersions.contains(i)) {
          withInline {
            sql(s"INSERT INTO $name VALUES ($i)")
          }
        } else {
          sql(s"INSERT INTO $name VALUES ($i)")
        }
      }

      val staleLog = deltaLogForName(name)
      assert(staleLog.unsafeVolatileSnapshot.version == ctx.staleVersion,
        s"expected stale v${ctx.staleVersion}, got v${staleLog.unsafeVolatileSnapshot.version}.")
      DeltaLog.clearCache()

      // Advance the true table past the pin through fresh instances (cache cleared above).
      ((ctx.staleVersion + 1) to ctx.latestCommitVersion).foreach { i =>
        if (ctx.cpProviderVersions.contains(i)) {
          withInline {
            sql(s"INSERT INTO $name VALUES ($i)")
          }
        } else {
          sql(s"INSERT INTO $name VALUES ($i)")
        }
      }

      // The handle must still be stale at the pin before update().
      assert(staleLog.unsafeVolatileSnapshot.version == ctx.staleVersion,
        s"handle must remain stale at v${ctx.staleVersion} before update(), but was at " +
          s"v${staleLog.unsafeVolatileSnapshot.version}.")

      val snapshotAfterUpdate = staleLog.update(catalogTableOpt = Some(catalogTableFor(name)))
      assertWarmMatchesCold(
        snapshotAfterUpdate,
        name,
        ctx.latestCommitVersion,
        ctx.expectedCpVersion,
        ctx.expectedTrailingDeltas)
    }
  }

  Seq(
    WarmUpdateContext(
      label = "after a checkpoint",
      staleVersion = 3,
      cpProviderVersions = Seq(1),
      latestCommitVersion = 3,
      expectedCpVersion = Some(1),
      expectedTrailingDeltas = Seq(2, 3)
    ),
    WarmUpdateContext(
      label = "on a checkpoint",
      staleVersion = 4,
      cpProviderVersions = Seq(1, 4),
      latestCommitVersion = 4,
      expectedCpVersion = Some(4),
      expectedTrailingDeltas = Seq.empty
    ),
    WarmUpdateContext(
      label = "long trailing deltas",
      staleVersion = 10,
      cpProviderVersions = Seq(1, 4),
      latestCommitVersion = 10,
      expectedCpVersion = Some(4),
      expectedTrailingDeltas = Seq(5, 6, 7, 8, 9, 10)
    )
  ).foreach { ctx =>
    test(s"[warm update] no-op on the same version: ${ctx.label}") {
      runWarmUpdateContext(ctx)
    }
  }

  Seq(
    WarmUpdateContext(
      label = "no new checkpoints",
      staleVersion = 3,
      cpProviderVersions = Seq(1),
      latestCommitVersion = 4,
      expectedCpVersion = Some(1),
      expectedTrailingDeltas = Seq(2, 3, 4)
    ),
    WarmUpdateContext(
      label = "one new checkpoint",
      staleVersion = 3,
      cpProviderVersions = Seq(1, 4),
      latestCommitVersion = 5,
      expectedCpVersion = Some(4),
      expectedTrailingDeltas = Seq(5)
    ),
    WarmUpdateContext(
      label = "multiple new checkpoints",
      staleVersion = 3,
      cpProviderVersions = Seq(1, 4, 7, 10),
      latestCommitVersion = 12,
      expectedCpVersion = Some(10),
      expectedTrailingDeltas = Seq(11, 12)
    )
  ).foreach { ctx =>
    test(
      s"[warm update] builds the correct latest snapshot: some=>some trailing deltas,${ctx.label}"
    ) {
      runWarmUpdateContext(ctx)
    }
  }

  Seq(
    WarmUpdateContext(
      label = "one new checkpoint",
      staleVersion = 3,
      cpProviderVersions = Seq(1, 4),
      latestCommitVersion = 4,
      expectedCpVersion = Some(4),
      expectedTrailingDeltas = Seq.empty
    ),
    WarmUpdateContext(
      label = "multiple new checkpoints",
      staleVersion = 3,
      cpProviderVersions = Seq(1, 4, 7, 10),
      latestCommitVersion = 10,
      expectedCpVersion = Some(10),
      expectedTrailingDeltas = Seq.empty
    )
  ).foreach { ctx =>
    test(
      s"[warm update] builds the correct latest snapshot: some=>none trailing deltas, ${ctx.label}"
    ) {
      runWarmUpdateContext(ctx)
    }
  }

  Seq(
    WarmUpdateContext(
      label = "no new checkpoints",
      staleVersion = 4,
      cpProviderVersions = Seq(1, 4),
      latestCommitVersion = 5,
      expectedCpVersion = Some(4),
      expectedTrailingDeltas = Seq(5)
    ),
    WarmUpdateContext(
      label = "one new checkpoint",
      staleVersion = 4,
      cpProviderVersions = Seq(1, 4, 7),
      latestCommitVersion = 8,
      expectedCpVersion = Some(7),
      expectedTrailingDeltas = Seq(8)
    ),
    WarmUpdateContext(
      label = "multiple new checkpoints",
      staleVersion = 4,
      cpProviderVersions = Seq(1, 4, 7, 10),
      latestCommitVersion = 12,
      expectedCpVersion = Some(10),
      expectedTrailingDeltas = Seq(11, 12)
    )
  ).foreach { ctx =>
    test(
      s"[warm update] builds the correct latest snapshot: none=>some trailing deltas, ${ctx.label}"
    ) {
      runWarmUpdateContext(ctx)
    }
  }

  Seq(
    WarmUpdateContext(
      label = "one new checkpoint",
      staleVersion = 4,
      cpProviderVersions = Seq(1, 4, 7),
      latestCommitVersion = 7,
      expectedCpVersion = Some(7),
      expectedTrailingDeltas = Seq.empty
    ),
    WarmUpdateContext(
      label = "multiple new checkpoints",
      staleVersion = 4,
      cpProviderVersions = Seq(1, 4, 7, 10),
      latestCommitVersion = 10,
      expectedCpVersion = Some(10),
      expectedTrailingDeltas = Seq.empty
    )
  ).foreach { ctx =>
    test(
      s"[warm update] builds the correct latest snapshot: none=>none trailing deltas, ${ctx.label}"
    ) {
      runWarmUpdateContext(ctx)
    }
  }

  // The above test harness doesn't cover the case of a checkpoint-less stale handle acquiring its
  // first AMT checkpoint through update(), for the sake of versioning simplicity.
  test("[warm update] a checkpoint-less stale handle acquires its first AMT checkpoint") {
    val name = "amt_warm_no_cp_to_amt"
    withTable(name) {
      createAMTTable(name, checkpointInterval = 3)
      sql(s"INSERT INTO $name VALUES (1)") // v1: before the first checkpoint -- no provider
      sql(s"INSERT INTO $name VALUES (2)") // v2: still before the first checkpoint -- no provider

      // Pin a stale handle at v2, which precedes the first checkpoint and has no AMT provider.
      val staleLog = deltaLogForName(name)
      assert(staleLog.unsafeVolatileSnapshot.version == 2,
        s"expected stale v2, got v${staleLog.unsafeVolatileSnapshot.version}.")
      assert(amtProvider(staleLog.unsafeVolatileSnapshot).isEmpty,
        "the stale handle must have no AMT checkpoint provider before update().")
      DeltaLog.clearCache()

      sql(s"INSERT INTO $name VALUES (3)") // v3: reaches the boundary
                                           // v4: OPTIMIZE CHECKPOINT (state@v3) -- the first AMT
      assert(deltaLogForName(name).update().version == 4, "the first deferred AMT must land at v4.")

      // The handle must still be stale at v2 before update().
      assert(staleLog.unsafeVolatileSnapshot.version == 2,
        s"handle must remain stale at v2 before update(), but was at " +
          s"v${staleLog.unsafeVolatileSnapshot.version}.")

      val warm = staleLog.update(catalogTableOpt = Some(catalogTableFor(name)))
      assertWarmMatchesCold(
        warm,
        tableName = name,
        expectedVersion = 4,
        expectedProviderVersion = Some(3),
        expectedTrailingDeltas = Seq(4))
    }
  }

  ///////////////////////////
  // Time travel
  ///////////////////////////

  /**
   * The master oracle for `getSnapshotAt(version)` on an AMT table. It asserts the installed AMT
   * checkpoint version, the trimmed trailing deltas, and the resolved last-manifest-commit
   * reference.
   *
   * Callers invoke this during table buildup and after later checkpoints have committed, so
   * discovery is checked against the checkpoints available at each stage.
   */
  private def assertGetSnapshotAt(
      deltaLog: DeltaLog,
      version: Long,
      expectedProviderVersion: Option[Long],
      expectedTrailingDeltas: Seq[Long],
      expectedLmc: Option[LastManifestCommit]): Snapshot = {
    val snapshot = deltaLog.getSnapshotAt(version)
    assert(snapshot.version == version, s"v$version: got snapshot version ${snapshot.version}.")
    assert(amtProvider(snapshot).map(_.version) == expectedProviderVersion,
      s"v$version: provider ${amtProvider(snapshot).map(_.version)} != $expectedProviderVersion.")
    assert(segmentDeltaVersions(snapshot) == expectedTrailingDeltas,
      s"v$version: trailing deltas ${segmentDeltaVersions(snapshot)} != $expectedTrailingDeltas.")
    assert(snapshot.lastManifestCommitOpt == expectedLmc,
      s"v$version: lmc ${snapshot.lastManifestCommitOpt} != $expectedLmc.")
    snapshot
  }

  /** Starts a checkpoint transaction, runs intervening writes, then commits the checkpoint. */
  private def commitCheckpointAfterWrites(
      deltaLog: DeltaLog,
      expectedReadVersion: Long,
      incremental: Boolean)(writes: => Unit): Unit = {
    val txn = deltaLog.startTransaction()
    assert(txn.readVersion == expectedReadVersion)
    writes
    val triggerName = if (incremental) {
      AMTTriggerMode.CheckpointIntervalIncremental.name
    } else {
      AMTTriggerMode.CheckpointIntervalFull.name
    }
    txn.commit(
      Seq.empty,
      DeltaOperations.OptimizeCheckpoint(incremental = incremental, triggerName = triggerName))
  }

  test("[time travel] resolves the content root discoverable as of each target version: deferred") {
    withTable("amt_tt_deferred") {
      val name = "amt_tt_deferred"
      createAMTTable(name, checkpointInterval = Int.MaxValue)
      val deltaLog = deltaLogForName(name)
      // Start each checkpoint transaction before intervening INSERTs to separate its content root
      // from its manifest commit. Append-only winners let the checkpoint retain its original tree.
      //   v0: CREATE
      //   v1..v3: INSERTs; start the full checkpoint transaction at v3
      //   v4: INSERT
      //   v5: full OPTIMIZE CHECKPOINT (state@v3)
      //   v6: INSERT; start the incremental checkpoint transaction at v6
      //   v7..v8: INSERTs
      //   v9: incremental OPTIMIZE CHECKPOINT (state@v6)

      // Build the first deferred checkpoint at v5, describing content root 3. Check older versions
      // before the second checkpoint exists.
      (1 to 3).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
      commitCheckpointAfterWrites(deltaLog, expectedReadVersion = 3, incremental = false) {
        sql(s"INSERT INTO $name VALUES (4)")
      }
      assert(deltaLog.update().version == 5)

      // v1: before the first content root @v3.
      // `computeLastCheckpointHintsForAMT` returns empty hints; reconstruct from version 0.
      assertGetSnapshotAt(
        deltaLog,
        version = 1,
        expectedProviderVersion = None,
        expectedTrailingDeltas = Seq(0L, 1L),
        expectedLmc = None)

      // v3: at the first content root @v3, but before its manifest commit v5.
      // `computeLastCheckpointHintsForAMT` returns empty because no manifest has committed yet.
      assertGetSnapshotAt(
        deltaLog,
        version = 3,
        expectedProviderVersion = None,
        expectedTrailingDeltas = Seq(0L, 1L, 2L, 3L),
        expectedLmc = None)

      // v4: after the first content root @v3, but before its manifest commit v5.
      // `computeLastCheckpointHintsForAMT` returns empty because no manifest has committed yet.
      val beforeFullManifest = assertGetSnapshotAt(
        deltaLog,
        version = 4,
        expectedProviderVersion = None,
        expectedTrailingDeltas = Seq(0L, 1L, 2L, 3L, 4L),
        expectedLmc = None)
      checkAnswer(
        deltaLog.createDataFrame(
          beforeFullManifest, beforeFullManifest.allFilesViaStateReconstruction.collect().toSeq),
        (1 to 4).map(i => Row(i)))

      // v5: at the latest table version.
      // Short-circuit to upper bound (latest) snapshot.
      val lmcAtV5 = LastManifestCommit(version = 5, contentRootVersion = 3)
      assertGetSnapshotAt(
        deltaLog,
        version = 5,
        expectedProviderVersion = Some(3L),
        expectedTrailingDeltas = Seq(4L, 5L),
        expectedLmc = Some(lmcAtV5))

      // The next checkpoint retains content root 6 across two intervening INSERTs.
      sql(s"INSERT INTO $name VALUES (6)")
      commitCheckpointAfterWrites(deltaLog, expectedReadVersion = 6, incremental = true) {
        (7 to 8).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
      }
      assert(deltaLog.update().version == 9)

      // After the second checkpoint, v1, v3 and v4 should behave identically, while v5 should start
      // calling `computeLastCheckpointHintsForAMT` to discover the content root @v3.
      assertGetSnapshotAt(
        deltaLog,
        version = 1,
        expectedProviderVersion = None,
        expectedTrailingDeltas = Seq(0L, 1L),
        expectedLmc = None)
      assertGetSnapshotAt(
        deltaLog,
        version = 3,
        expectedProviderVersion = None,
        expectedTrailingDeltas = Seq(0L, 1L, 2L, 3L),
        expectedLmc = None)
      assertGetSnapshotAt(
        deltaLog,
        version = 4,
        expectedProviderVersion = None,
        expectedTrailingDeltas = Seq(0L, 1L, 2L, 3L, 4L),
        expectedLmc = None)
      assertGetSnapshotAt(
        deltaLog,
        version = 5,
        expectedProviderVersion = Some(3L),
        expectedTrailingDeltas = Seq(4L, 5L),
        expectedLmc = Some(lmcAtV5))

      // v6 and v8: normal discovery follows the carried-forward LMC (5, 3) to provider @3.
      // `computeLastCheckpointHintsForAMT` returns hints for content root @v3.
      assertGetSnapshotAt(
        deltaLog,
        version = 6,
        expectedProviderVersion = Some(3L),
        expectedTrailingDeltas = Seq(4L, 5L, 6L),
        expectedLmc = Some(lmcAtV5))
      assertGetSnapshotAt(
        deltaLog,
        version = 8,
        expectedProviderVersion = Some(3L),
        expectedTrailingDeltas = Seq(4L, 5L, 6L, 7L, 8L),
        expectedLmc = Some(lmcAtV5))

      // v9: at the latest table version.
      // Short-circuit to upper bound (latest) snapshot.
      val lmcAtV9 = LastManifestCommit(version = 9, contentRootVersion = 6)
      assertGetSnapshotAt(
        deltaLog,
        version = 9,
        expectedProviderVersion = Some(6L),
        expectedTrailingDeltas = Seq(7L, 8L, 9L),
        expectedLmc = Some(lmcAtV9))
    }
  }

  testInline("[time travel] resolves the content root discoverable as of each target version") {
    withTable("amt_tt_inline") {
      val name = "amt_tt_inline"
      createAMTTable(name, checkpointInterval = 2)
      val deltaLog = deltaLogForName(name)
      // Inline, interval 2 (see the inline cold test for the full lifecycle rationale):
      //   v0: CREATE
      //   v1: INSERT 1                       (no full AMT yet -> cannot inline)
      //   v2: INSERT 2 (reaches boundary)    (the first AMT still cannot inline)
      //   v3: OPTIMIZE CHECKPOINT (state@v2) (the first, full AMT; deferred)
      //   v4: INSERT 3 + inline AMT (state@v4)
      //   v5: INSERT 4 + inline AMT (state@v5)

      // Build up to the first (deferred) AMT at v3.
      (1 to 2).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
      assert(deltaLog.update().version == 3)

      // v1: before the first manifest commit -> reconstruct from 0, no provider.
      assertGetSnapshotAt(
        deltaLog,
        version = 1,
        expectedProviderVersion = None,
        expectedTrailingDeltas = Seq(0L, 1L),
        expectedLmc = None)

      // v3: the deferred first AMT. lmc-at-3 = (3, 2): the content root is behind the recording
      // version -> provider @2, trailing delta [3].
      val lmcAtV3 = LastManifestCommit(version = 3, contentRootVersion = 2)
      assertGetSnapshotAt(
        deltaLog,
        version = 3,
        expectedProviderVersion = Some(2L),
        expectedTrailingDeltas = Seq(3L),
        expectedLmc = Some(lmcAtV3))

      // Advance so each subsequent business commit inlines its own manifest.
      (3 to 4).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
      assert(deltaLog.update().version == 5)

      // v4: an inline manifest commit -- version and content root coincide (4, 4) -> provider @4,
      // no trailing deltas.
      val lmcAtV4 = LastManifestCommit(version = 4, contentRootVersion = 4)
      assertGetSnapshotAt(
        deltaLog,
        version = 4,
        expectedProviderVersion = Some(4L),
        expectedTrailingDeltas = Seq.empty,
        expectedLmc = Some(lmcAtV4))

      // v5 == the latest version: getSnapshotAt short-circuits to the current snapshot, the inline
      // manifest at (5, 5) -> provider @5, no trailing deltas.
      val lmcAtV5 = LastManifestCommit(version = 5, contentRootVersion = 5)
      assertGetSnapshotAt(
        deltaLog,
        version = 5,
        expectedProviderVersion = Some(5L),
        expectedTrailingDeltas = Seq.empty,
        expectedLmc = Some(lmcAtV5))
    }
  }

  ///////////////////////////
  // Post commit snapshot
  ///////////////////////////

  testAcrossAMTCheckpointScenarios(
      "emission installs an AMTCheckpointProvider on the post-commit snapshot",
      "amt_provider_install")(
      setup = name => sql(s"INSERT INTO $name VALUES (1)"),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (2)"))) { context =>
    // The harness checks the provider on the snapshot it re-read from the log. This test covers the
    // post-commit path specifically: the emission must install the provider on the in-memory
    // `unsafeVolatileSnapshot` the commit produced, without waiting for a fresh log read.
    val postCommit = context.postCheckpointSnapshot.deltaLog.unsafeVolatileSnapshot
    assert(postCommit.version == context.manifestCommitVersion,
      s"The post-commit snapshot must be at v${context.manifestCommitVersion}; " +
        s"got v${postCommit.version}.")
    val provider = amtProvider(postCommit).getOrElse(
      fail("The post-commit snapshot must expose an AMTCheckpointProvider."))
    assert(provider.checkpointVersion == context.checkpoint.version,
      s"The provider must describe v${context.checkpoint.version}; " +
        s"got v${provider.checkpointVersion}.")
    assert(provider.checkpointAction.contentRoot.path == context.checkpoint.contentRoot.path,
      "The provider must point at the emitted checkpoint's root manifest.")
  }

  testAcrossAMTCheckpointScenarios(
      "an emitted AMT installs the provider and trims the log segment",
      "amt_log_segment")(
      setup = name => (1 to 2).foreach(i => sql(s"INSERT INTO $name VALUES ($i)")),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (3)"))) { context =>
    val segmentDeltaVersions =
      context.postCheckpointSnapshot.logSegment.deltas.map(f => FileNames.deltaVersion(f))
    assert(segmentDeltaVersions.forall(_ > context.checkpoint.version),
      s"Log segment must trim deltas up to the checkpoint version; got $segmentDeltaVersions.")
  }

  test("[post-commit] conflict retry reuses the AMT discovered on a separate DeltaLog") {
    // Time  Transaction A                             Transaction B
    // ----  ----------------------------------------  -------------------------------------
    // T0    Initial: table v2; AMT covers v1
    // T1    Reads v2; starts at readVersion = 2
    // T2                                              Separate DeltaLog commits AMT at v3
    //                                                 AMT covers v2; manifest commit is v3
    // T3    Still holds v2; attempts commit
    // T4    Detects v3 conflict; loads v2 AMT provider once
    // T5    Rebases and commits at v4
    // T6    Builds v4; reuses provider; deltas v3-v4
    withSQLConf(leafPackingConfs: _*) {
      val name = "amt_post_commit_conflict_reuses_provider"
      withTable(name) {
        createAMTTable(name, checkpointInterval = Int.MaxValue)
        appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles)
        val oldDeltaLog = deltaLogForName(name)
        commitCheckpoint(oldDeltaLog, incremental = false)
        val readSnapshot = oldDeltaLog.unsafeVolatileSnapshot
        assert(readSnapshot.version == 2)
        val readProvider = amtProvider(readSnapshot).get
        assert(readProvider.version == 1)
        val txn = oldDeltaLog.startTransaction(Some(catalogTableFor(name)))
        assert(txn.readVersion == 2)

        DeltaLog.clearCache()
        val concurrentDeltaLog = deltaLogForName(name)
        assert(concurrentDeltaLog ne oldDeltaLog)
        commitCheckpoint(concurrentDeltaLog, incremental = false)
        val winningSnapshot = concurrentDeltaLog.unsafeVolatileSnapshot
        assert(winningSnapshot.version == 3)
        val winningProvider = amtProvider(winningSnapshot).get
        val winningCheckpoint = winningProvider.checkpointAction
        assert(winningProvider.version == 2)
        assertLeafCount(winningProvider.leaves)
        assert(oldDeltaLog.unsafeVolatileSnapshot eq readSnapshot)

        // This synthetic MERGE bypasses the command that normally registers its SQL metrics.
        txn.registerSQLMetrics(spark, Map(
          "operationNumSourceRows" -> SQLMetrics.createMetric(spark.sparkContext, "source rows")))
        val initializationEvents = collectUsageLogs(
            AMTUsageLogs.CHECKPOINT_PROVIDER_INITIALIZE_FROM_CHECKPOINT_ACTION) {
          assert(txn.commit(Seq.empty, DeltaOperations.Merge(None, Nil, Nil, Nil)) == 4)
        }
        // The retry must load the winning AMT once; post-commit must reuse that provider.
        assert(initializationEvents.size == 1)
        val metrics = JsonUtils.fromJson[Map[String, Long]](initializationEvents.head.blob)
        assert(metrics("durationMs") >= 0)
        assert(metrics("numLeaves") == winningProvider.leaves.size.toLong)
        assert(metrics("contentRootSizeInBytes") == winningCheckpoint.contentRoot.sizeInBytes)
        assert(metrics("manifestCommitVersion") == 3)
        assert(metrics("contentRootVersion") == 2)
        assert(metrics("checkpointVersion") == 2)

        val postCommitSnapshot = oldDeltaLog.unsafeVolatileSnapshot
        assert(postCommitSnapshot.version == 4)
        assert(postCommitSnapshot eq txn.getCommitted.get.postCommitSnapshot)
        val provider = amtProvider(postCommitSnapshot).get
        assert(provider.version == 2)
        assert(provider.manifestCommitVersion == 3)
        assert(provider.checkpointAction == winningCheckpoint)
        assert(provider.leaves == winningProvider.leaves)
        assert(postCommitSnapshot.logSegment.deltas.map(FileNames.deltaVersion) == Seq(3L, 4L))
      }
    }
  }

  //////////////////////////////////
  // Minor compaction compatibility
  //////////////////////////////////

  private def buildDeltaLogWithCompactedDeltasAndStaleLastCheckpointHint(name: String)
    : (DeltaLog, Snapshot, DeltaLog) = {
    createAMTTable(name, checkpointInterval = 2)
    // v1: INSERT (1)
    // v2: INSERT (2)
    // v3: OPTIMIZE CHECKPOINT (content root @v2)
    (1 to 2).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
    assert(deltaLogForName(name).unsafeVolatileSnapshot.version == 3)

    val staleHintAtV2 = readLastCheckpointBytes(name)
    val staleLogAtV3 = deltaLogForName(name)
    assert(staleLogAtV3.unsafeVolatileSnapshot.version == 3)
    DeltaLog.clearCache()

    // v4: INSERT (3)
    // v5: OPTIMIZE CHECKPOINT (content root @v4)
    sql(s"INSERT INTO $name VALUES (3)")
    assert(deltaLogForName(name).unsafeVolatileSnapshot.version == 5)

    // v6: Bump up checkpoint interval
    // v7: INSERT (4)
    // v8: INSERT (5)
    sql(s"ALTER TABLE $name SET TBLPROPERTIES ('delta.checkpointInterval' = '1000')")
    (4 to 5).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))

    val latestDeltaLogAtV8 = deltaLogForName(name)
    val latestSnapshotAtV8 = latestDeltaLogAtV8.update()
    assert(latestSnapshotAtV8.version == 8)
    assert(amtProvider(latestSnapshotAtV8).map(_.version).contains(4L))
    assert(staleLogAtV3.unsafeVolatileSnapshot.version == 3)

    // Deltas layout: 1, 2, [3, 4, 5], [6, 7, 8]
    // Stale CP hint:   <2>
    // Stale deltas:        [3, 4, 5], [6, 7, 8])
    // Latest CP:              <4>
    // Trimmed deltas:             5,  [6, 7, 8]
    // (Stale means that this is the initial LogSegment built by `getLogSegmentForVersion`,
    // which is not yet trimmed to the accurate checkpoint version by AMT reconciliation.)
    Seq((3, 5), (6, 8)).foreach { case (start, end) =>
      minorCompactDeltaLog(
        tablePath = latestDeltaLogAtV8.dataPath.toString,
        startVersion = start,
        endVersion = end,
        tableName = Some(name))
    }
    overwriteLastCheckpoint(name, staleHintAtV2)

    (latestDeltaLogAtV8, latestSnapshotAtV8, staleLogAtV3)
  }

  private def assertSnapshotTrimmed(
      snapshot: Snapshot,
      expectedVersion: Long,
      expectedAMTContentRootVersion: Long,
      expectedIndividualDeltas: Seq[Long],
      expectedCompactedDeltas: Seq[(Long, Long)],
      expectedNonCompactedDeltas: Seq[Long],
      expectedData: Set[Int]): Unit = {
    assert(snapshot.version == expectedVersion)
    assert(amtProvider(snapshot).map(_.version).contains(expectedAMTContentRootVersion))
    val actualIndividualDeltas = snapshot.logSegment.deltas
      .filterNot(FileNames.isCompactedDeltaFile)
      .map(FileNames.deltaVersion)
    assert(actualIndividualDeltas == expectedIndividualDeltas)
    val actualCompactedDeltas = snapshot.logSegment.deltas
      .filter(FileNames.isCompactedDeltaFile)
      .map(FileNames.compactedDeltaVersions)
    assert(actualCompactedDeltas == expectedCompactedDeltas)
    val actualNonCompactedDeltas = snapshot.logSegment.nonCompactedDeltasOpt
      .map(n => n.map(FileNames.deltaVersion))
    assert(actualNonCompactedDeltas.contains(expectedNonCompactedDeltas))
    val actualDeltaAtCheckpointVersion = snapshot.logSegment.deltaAtCheckpointVersionOpt
      .map(FileNames.deltaVersion)
    assert(actualDeltaAtCheckpointVersion.contains(expectedAMTContentRootVersion))
    val reconstructedData = snapshot.deltaLog
      .createDataFrame(snapshot, snapshot.allFilesViaStateReconstruction.collect().toSeq)
      .collect().map(_.getInt(0)).toSet
    assert(reconstructedData == expectedData)
  }

  test("[minor compaction] a straddling compacted delta is dropped and its gap refilled: cold") {
    val name = "amt_compaction_straddle_cold"
    withTable(name) {
      val (latestDeltaLogAtV8, latestSnapshotAtV8, _) =
        buildDeltaLogWithCompactedDeltasAndStaleLastCheckpointHint(name)
      // Cold path: a cold load builds the segment at content root 2 (stale hint) first, and then
      // reconciles to trim to content root 4.
      val (coldDeltaLog, coldSnapshot) = coldLoad(name)
      assertSnapshotTrimmed(
        coldSnapshot,
        expectedVersion = 8,
        expectedAMTContentRootVersion = 4,
        expectedIndividualDeltas = Seq(5L),
        expectedCompactedDeltas = Seq((6L, 8L)),
        expectedNonCompactedDeltas = Seq(5L, 6L, 7L, 8L),
        expectedData = Set(1, 2, 3, 4, 5))
    }
  }

  test("[minor compaction] a straddling compacted delta is dropped and its gap refilled: warm") {
    val name = "amt_compaction_straddle_warm"
    withTable(name) {
      val (latestDeltaLogAtV8, latestSnapshotAtV8, staleLogAtV3) =
        buildDeltaLogWithCompactedDeltasAndStaleLastCheckpointHint(name)
      // Warm path: a warm update builds the segment at content root 2 (stale hint + old checkpoint
      // provider reuse) first, and then reconciles to trim to content root 4.
      val warmSnapshot = staleLogAtV3.update(catalogTableOpt = Some(catalogTableFor(name)))
      assert(warmSnapshot.version == 8, s"expected warm v8, got v${warmSnapshot.version}.")
      assertSnapshotTrimmed(
        warmSnapshot,
        expectedVersion = 8,
        expectedAMTContentRootVersion = 4,
        expectedIndividualDeltas = Seq(5L),
        expectedCompactedDeltas = Seq((6L, 8L)),
        expectedNonCompactedDeltas = Seq(5L, 6L, 7L, 8L),
        expectedData = Set(1, 2, 3, 4, 5))
    }
  }

  test("[minor compaction] the post-commit fast path trims a compacted pre-commit segment") {
    val name = "amt_compaction_post_commit"
    withTable(name) {
      val (latestDeltaLog, latestSnapshotAtV8, _) =
        buildDeltaLogWithCompactedDeltasAndStaleLastCheckpointHint(name)
      // Emit a deferred full checkpoint. All the previous deltas (compacted or not) will be trimmed
      // as the checkpoint is installed. A compacted delta will never straddle on this content root,
      // unless there has been concurrent updates on the table.
      commitCheckpoint(latestDeltaLog, incremental = false) // V9: OPTIMIZE CHECKPOINT (AMT @V8)
      val postCommitSnapshot = latestDeltaLog.unsafeVolatileSnapshot
      assertSnapshotTrimmed(
        postCommitSnapshot,
        expectedVersion = 9L,
        expectedAMTContentRootVersion = 8L,
        expectedIndividualDeltas = Seq(9L),
        expectedCompactedDeltas = Seq.empty,
        expectedNonCompactedDeltas = Seq(9L),
        expectedData = Set(1, 2, 3, 4, 5))
    }
  }

  test("[minor compaction] incremental checkpoint replays a compacted pre-commit segment") {
    val name = "amt_compaction_incremental"
    withTable(name) {
      val (_, _, _) = buildDeltaLogWithCompactedDeltasAndStaleLastCheckpointHint(name)
      // Cold load the delta log to incorporate the compacted deltas into the snapshot.
      val (coldDeltaLog, _) = coldLoad(name)
      commitCheckpoint(coldDeltaLog, incremental = true)
      val postCommitSnapshot = coldDeltaLog.unsafeVolatileSnapshot
      assertSnapshotTrimmed(
        postCommitSnapshot,
        expectedVersion = 9L,
        expectedAMTContentRootVersion = 8L,
        expectedIndividualDeltas = Seq(9L),
        expectedCompactedDeltas = Seq.empty,
        expectedNonCompactedDeltas = Seq(9L),
        expectedData = Set(1, 2, 3, 4, 5))
    }
  }

  ///////////////////////////
  // Usage log emission
  ///////////////////////////

  test("happy-paths should reuses AMT leaves without initializing from a checkpoint action") {
    def assertNoCheckpointInitialization(stage: String)(operation: => Unit): Unit = {
      val initializationEvents = collectUsageLogs(
        AMTUsageLogs.CHECKPOINT_PROVIDER_INITIALIZE_FROM_CHECKPOINT_ACTION)(operation)
      assert(initializationEvents.isEmpty, s"$stage re-read the AMT root: $initializationEvents")
    }

    withSQLConf(leafPackingConfs: _*) {
      val name = "amt_happy_paths_reuse_leaves"
      withTable(name) {
        createAMTTable(name, checkpointInterval = Int.MaxValue)
        appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles)
        val deltaLog = deltaLogForName(name)
        assert(deltaLog.unsafeVolatileSnapshot.version == 1)
        assert(amtProvider(deltaLog.unsafeVolatileSnapshot).isEmpty)

        assertNoCheckpointInitialization("First AMT") {
          commitCheckpoint(deltaLog, incremental = false)
        }
        val firstAMTPostCommitSnapshot = deltaLog.unsafeVolatileSnapshot
        assert(firstAMTPostCommitSnapshot.version == 2)
        val firstAMTCpProvider = amtProvider(firstAMTPostCommitSnapshot).get
        assert(firstAMTCpProvider.manifestCommitVersion == 2)
        assert(firstAMTCpProvider.version == 1)
        assertLeafCount(firstAMTCpProvider.leaves)

        assertNoCheckpointInitialization("Log commit") {
          sql(s"INSERT INTO $name VALUES (1)")
        }
        val logPostCommitSnapshot = deltaLog.unsafeVolatileSnapshot
        assert(logPostCommitSnapshot.version == 3)
        // The log commit must reuse the same AMT checkpoint provider as the first AMT commit.
        val logCpProvider = amtProvider(logPostCommitSnapshot).get
        assert(logCpProvider eq firstAMTCpProvider)
        assert(logCpProvider.manifestCommitVersion == 2)
        assert(logCpProvider.version == 1)

        assertNoCheckpointInitialization("Second AMT") {
          withInline {
            sql(s"INSERT INTO $name VALUES (2)")
          }
        }
        val secondAMTPostCommitSnapshot = deltaLog.unsafeVolatileSnapshot
        assert(secondAMTPostCommitSnapshot.version == 4)
        val secondAMTCpProvider = amtProvider(secondAMTPostCommitSnapshot).get
        assert(secondAMTCpProvider.manifestCommitVersion == 4)
        assert(secondAMTCpProvider.version == 4)
        assertLeafCount(secondAMTCpProvider.leaves)

      }
    }
  }
}

/**
 * AMT snapshot discovery must work identically via the CommitInfo fallback when CRC is missing.
 */
class AMTSnapshotDiscoveryWithoutCRCSuite extends AMTSnapshotDiscoverySuite {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, "false")

  test("[cold init] refuses an AMT provider when neither CRC nor CommitInfo carries a reference") {
    val name = "amt_no_reference_refused"
    withTable(name) {
      createAMTTable(name, checkpointInterval = 2)
      // 2 INSERTs land the v2 tree recorded at v3, so the hint references an AMT checkpoint
      // that a cold read installs as the provider.
      (1 to 2).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))

      // Strip the reference from the recording commit's CommitInfo. With no CRC and no CommitInfo
      // reference, nothing corroborates the installed AMT provider, so the cold read is refused.
      val (deltaLog, snapshot) = coldLoad(name)
      overwriteCommitInfoLastManifestCommit(
        deltaLog, snapshot, version = 3, lastManifestCommitOpt = None)

      val e = intercept[IllegalStateException](coldLoad(name))
      assert(e.getMessage.contains("no lastManifestCommit is available from either the CRC"),
        s"expected the no-reference refusal, got: ${e.getMessage}")
    }
  }

  test("[cold init] CommitInfo read is not kicked off when config is disabled: AMT table") {
    withSQLConf(
        DeltaSQLConf.AMT_SNAPSHOT_DISCOVERY_ASYNC_COMMIT_INFO_READ_ENABLED.key -> "false") {
      val name = "amt_slow_commit_info_read"
      withTable(name) {
        createAMTTable(name, checkpointInterval = 2)
        // 2 INSERTs land the v2 tree recorded at v3, so the hint references an AMT checkpoint
        // that a cold read installs as the provider.
        (1 to 2).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))

        // With CommitInfo read not kicked off, the installed AMT provider is refused.
        val e = intercept[IllegalStateException](coldLoad(name))
        assert(e.getMessage.contains("no lastManifestCommit is available from either the CRC"))
      }
    }
  }

  test("[cold init] CommitInfo read is not kicked off when config is disabled: non-AMT table") {
    withSQLConf(
        DeltaSQLConf.AMT_SNAPSHOT_DISCOVERY_ASYNC_COMMIT_INFO_READ_ENABLED.key -> "false") {
      val name = "non_amt_slow_commit_info_read"
      withTable(name) {
        sql(s"CREATE TABLE $name (id INT) USING delta")
        (1 to 2).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))

        // With CommitInfo read not kicked off, non-AMT tables can be loaded unaffected.
        val (_, snapshot) = coldLoad(name)
        assert(snapshot.version == 2, "the snapshot must be at v2.")
        assert(amtProvider(snapshot).isEmpty, "the AMT provider must be absent.")
      }
    }
  }
}

/**
 * With batch size 1, all commits are backfilled to a standard NNN.json immediately.
 */
class AMTSnapshotDiscoveryBackfillBatch1Suite extends AMTSnapshotDiscoverySuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)
}

/**
 * With a large backfill batch size, no commits are automatically backfilled. Only before an AMT
 * checkpoint lands in a manifest commit, the previous commits are backfilled. This suite exercises
 * that code path and ensures AMT snapshot discovery works correctly.
 */
class AMTSnapshotDiscoveryBackfillBatch100Suite extends AMTSnapshotDiscoverySuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(100)
}
