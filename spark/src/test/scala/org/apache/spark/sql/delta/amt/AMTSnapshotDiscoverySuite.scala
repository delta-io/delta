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

import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, CommitInfo, LastManifestCommit}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, FileNames}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable

class AMTSnapshotDiscoverySuite extends AMTCheckpointTestBase {

  /** Whether this suite runs with `.crc` files enabled, read from the effective conf. */
  protected def writeChecksumEnabled: Boolean =
    spark.conf.get(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED)

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
      val deltaLog = deltaLogForName(name)
      val commitPath = DeltaCommitFileProvider(deltaLog.unsafeVolatileSnapshot).deltaFile(3)
      val hadoopConf = deltaLog.newDeltaHadoopConf()
      val stripped = deltaLog.store.readAsIterator(commitPath, hadoopConf).toList
        .map(Action.fromJson)
        .map {
          case ci: CommitInfo => ci.copy(lastManifestCommit = None).json
          case other => other.json
        }
      deltaLog.store.write(commitPath, stripped.toIterator, overwrite = true, hadoopConf)

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
