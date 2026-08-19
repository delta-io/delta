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
import org.apache.spark.sql.delta.actions.LastManifestCommit
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.catalyst.TableIdentifier

class AMTSnapshotDiscoverySuite extends AMTCheckpointTestBase {

  ////////////////////////////
  // Cold snapshot discovery
  ////////////////////////////

  /** Loads a genuinely cold deltaLog + snapshot (cache cleared) for the given table. */
  private def coldLoad(tableName: String): (DeltaLog, Snapshot) = {
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
    // The AMT checkpoint is only trusted once the CRC corroborates it, so a cold read of an AMT
    // table always resolves a checksum carrying the same manifest-commit reference.
    val checksum = snapshot.checksumOpt.getOrElse(
      fail(s"v$version: a cold AMT read must resolve a checksum (CRC)."))
    assert(checksum.lastManifestCommit == lastManifestCommit,
      s"v$version: the CRC's manifest-commit reference must match the snapshot's.")
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

  test("[cold init] refuses any AMT checkpoint provider without a CRC to cross-verify") {
    withSQLConf(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "false") {
      val name = "amt_no_crc_refused"
      withTable(name) {
        createAMTTable(name, checkpointInterval = 2)
        // 2 INSERTs land the v2 tree recorded at v3, so the hint references an AMT checkpoint that
        // a cold read will install as the provider. Commits reuse the in-memory post-commit
        // snapshot (which does not verify), so they succeed even though no CRC is written.
        (1 to 2).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))

        // A cold read installs the AMT provider from the hint, finds no CRC, and refuses it. The
        // intercept itself proves the provider was installed (otherwise there would be no throw).
        val e = intercept[IllegalStateException](coldLoad(name))
        assert(e.getMessage.contains("no checksum (CRC) file is available to corroborate it"),
          s"expected the no-CRC refusal, got: ${e.getMessage}")
      }
    }
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
