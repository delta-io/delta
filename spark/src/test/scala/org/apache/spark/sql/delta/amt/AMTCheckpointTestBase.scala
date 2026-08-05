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

import java.io.File

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{AdaptiveMetadataTableFeature, DeltaLog, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile, Checkpoint, RemoveFile}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col

/**
 * Shared fixtures for AMT (`adaptiveMetadata-preview`) test suites: table creation, manifest-tree
 * file lookups, and typed access to the snapshot's [[AMTCheckpointProvider]].
 *
 * AMT requires the `catalogManaged` feature, so tables must be catalog-managed and accessed by
 * name (path-based access is blocked). This mixes in [[CatalogOwnedTestBaseSuite]] to register an
 * in-memory commit coordinator and creates/accesses tables by name.
 */
trait AMTCheckpointTestBase
  extends QueryTest
  with CatalogOwnedTestBaseSuite
  with DeltaSQLCommandTest {

  // Register the in-memory commit coordinator so catalog-managed AMT tables can be created locally.
  // Backfill batch size 1 so every commit is backfilled to a standard NNN.json immediately (rather
  // than staying as a UUID-named staged commit); the suites read commit actions via
  // `deltaLog.getChanges`, which only sees backfilled deltas.
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key, "false")

  /** Typed view of a snapshot's checkpoint provider when it is AMT-backed. */
  protected def amtProvider(snapshot: Snapshot): Option[AMTCheckpointProvider] =
    snapshot.checkpointProvider match {
      case amt: AMTCheckpointProvider => Some(amt)
      case _ => None
    }

  /** The [[DeltaLog]] for a catalog-managed table accessed by name. */
  protected def deltaLogForName(tableName: String): DeltaLog =
    DeltaLog.forTable(spark, new TableIdentifier(tableName))

  /** The physical data path of a catalog-managed table accessed by name. */
  protected def tablePath(tableName: String): String =
    new File(deltaLogForName(tableName).dataPath.toUri).getCanonicalPath

  protected def createAMTTable(
      tableName: String,
      checkpointInterval: Int = 2,
      location: Option[String] = None): Unit = {
    val locationClause = location.map(l => s"LOCATION '$l'").getOrElse("")
    sql(
      s"""CREATE TABLE $tableName (id INT) USING DELTA
         |$locationClause
         |TBLPROPERTIES (
         |  '${propertyKey(AdaptiveMetadataTableFeature)}' = '$FEATURE_PROP_SUPPORTED',
         |  'delta.columnMapping.mode' = 'id',
         |  'delta.enableDeletionVectors' = 'true',
         |  'delta.checkpointInterval' = '$checkpointInterval')""".stripMargin)
  }

  /**
   * Appends `numRows` rows (ids `startId` until `startId + numRows`) to `tableName` as `numRows`
   * separate data files in a single commit. `startId` lets successive calls append disjoint id
   * ranges.
   */
  protected def appendRowsAsSeparateFiles(
      tableName: String, numRows: Int, startId: Int = 0): Unit = {
    withSQLConf(
        "spark.sql.files.maxRecordsPerFile" -> "1",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false") {
      sql(
        s"INSERT INTO $tableName SELECT CAST(id AS INT) FROM range($startId, ${startId + numRows})")
    }
  }

  /** A production-supported combination of AMT placement and materialization strategy. */
  protected sealed abstract class AMTCheckpointScenario(
      val name: String,
      val isInline: Boolean,
      val isIncremental: Boolean)

  protected object AMTCheckpointScenario {
    case object InlineIncremental extends AMTCheckpointScenario(
      "inline incremental", isInline = true, isIncremental = true)
    case object DeferredIncremental extends AMTCheckpointScenario(
      "deferred incremental", isInline = false, isIncremental = true)
    case object DeferredFull extends AMTCheckpointScenario(
      "deferred full", isInline = false, isIncremental = false)
  }

  /**
   * The business commit a test uses to trigger a checkpoint, as a function of the table name:
   * either actions to commit through a transaction, or a SQL statement to run.
   */
  protected type AMTCheckpointTrigger =
    String => Either[(Seq[Action], DeltaOperations.Operation), String]

  /**
   * Normalized result of producing one checkpoint through a supported production path.
   *
   * @param scenario the placement and materialization strategy used by the test
   * @param tableName the catalog-managed table created by the harness
   * @param preCheckpointSnapshot the snapshot after `setup` and before the tested checkpoint
   * @param manifestCommitVersion the commit carrying `checkpoint`; this equals
   *                              `checkpoint.version` for inline checkpoints and
   *                              `checkpoint.version + 1` for deferred checkpoints
   * @param checkpoint the Checkpoint action produced by the scenario
   * @param provider the AMT provider installed from `checkpoint`
   * @param postCheckpointSnapshot the snapshot after the checkpoint commit
   */
  protected case class AMTCheckpointScenarioContext(
      scenario: AMTCheckpointScenario,
      tableName: String,
      preCheckpointSnapshot: Snapshot,
      manifestCommitVersion: Long,
      checkpoint: Checkpoint,
      provider: AMTCheckpointProvider,
      postCheckpointSnapshot: Snapshot)

  /**
   * Registers one test for each requested production-supported checkpoint scenario.
   *
   * For each scenario, the harness creates an AMT table and a full bootstrap checkpoint, then runs
   * `setup`.
   *
   *  - When `inlineCheckpointTriggerActionsOrSQL` is defined, every scenario executes that
   *    table-name-aware business commit and the harness registers an
   *    [[AMTCheckpointScenario.InlineIncremental]] test, which sets the inline action threshold
   *    to one for the commit.
   *  - For [[AMTCheckpointScenario.DeferredIncremental]] and
   *    [[AMTCheckpointScenario.DeferredFull]], the harness commits an explicit incremental or full
   *    OPTIMIZE CHECKPOINT after the business commit, respectively.
   *
   * Finally, the harness calls `body` with an [[AMTCheckpointScenarioContext]] that normalizes
   * inline and deferred version placement. Tests can assert on the checkpointed business commit,
   * the manifest commit, the resulting Checkpoint action and provider, and the final snapshot
   * without duplicating scenario-specific version arithmetic.
   */
  protected def testAcrossAMTCheckpointScenarios(
      testName: String,
      tableName: String,
      deferredScenarios: Seq[AMTCheckpointScenario] = Seq(
        AMTCheckpointScenario.DeferredIncremental,
        AMTCheckpointScenario.DeferredFull),
      sqlConfs: Seq[(String, String)] = Seq.empty)(
      setup: String => Unit = _ => (),
      inlineCheckpointTriggerActionsOrSQL: Option[AMTCheckpointTrigger] = None)(
      body: AMTCheckpointScenarioContext => Unit): Unit = {
    require(!deferredScenarios.contains(AMTCheckpointScenario.InlineIncremental),
      "deferredScenarios must contain only deferred checkpoint scenarios")
    val scenarios = inlineCheckpointTriggerActionsOrSQL
      .map(_ => AMTCheckpointScenario.InlineIncremental).toSeq ++ deferredScenarios
    scenarios.foreach { scenario =>
      test(s"$testName (${scenario.name})") {
        // Each scenario gets its own table, so it never inherits the previous scenario's storage.
        // `withTable` only drops the catalog entry; for these catalog-managed tables the files can
        // still be settling (commit backfill runs on a background pool), so a shared name lets one
        // scenario's leftovers fail the next one's CREATE with
        // DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION.
        val scenarioTable = s"${tableName}_${scenario.name.replace(' ', '_')}"
        withSQLConf(sqlConfs: _*) {
          withTable(scenarioTable) {
            createAMTTable(scenarioTable, checkpointInterval = Int.MaxValue)
            // Incremental checkpoints require an existing full checkpoint.
            commitCheckpoint(deltaLogForName(scenarioTable), incremental = false)
            setup(scenarioTable)
            val context = initScenarioAndBuildContext(
              scenarioTable, scenario, inlineCheckpointTriggerActionsOrSQL)
            assertAMTCheckpointScenarioInvariants(context)
            body(context)
          }
        }
      }
    }
  }

  private def initScenarioAndBuildContext(
      tableName: String,
      scenario: AMTCheckpointScenario,
      inlineCheckpointTriggerActionsOrSQL: Option[AMTCheckpointTrigger])
      : AMTCheckpointScenarioContext = {
    import AMTCheckpointScenario._

    val deltaLog = deltaLogForName(tableName)
    val preCheckpointSnapshot = deltaLog.update()

    def runCheckpointTrigger(): Unit = {
      inlineCheckpointTriggerActionsOrSQL.map(_(tableName)).foreach {
        case Left((actions, operation)) =>
          val txn = deltaLog.startTransaction()
          txn.commit(actions, operation)
        case Right(sqlText) => spark.sql(sqlText)
      }
    }

    scenario match {
      case InlineIncremental =>
        withSQLConf(
            DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key
              -> "1") {
          runCheckpointTrigger()
        }
      case _ =>
        runCheckpointTrigger()
        commitCheckpoint(deltaLog, incremental = scenario.isIncremental)
    }

    val checkpointedVersion = preCheckpointSnapshot.version +
      (if (inlineCheckpointTriggerActionsOrSQL.isDefined) 1L else 0L)
    val manifestCommitVersion = checkpointedVersion + (if (scenario.isInline) 0L else 1L)
    val postCheckpointSnapshot = deltaLog.update()
    val checkpoint = checkpointAt(deltaLog, manifestCommitVersion).getOrElse(
      fail(s"${scenario.name}: expected a Checkpoint at v$manifestCommitVersion"))
    val provider = amtProvider(postCheckpointSnapshot).getOrElse(
      fail(s"${scenario.name}: post-checkpoint snapshot has no AMTCheckpointProvider"))
    AMTCheckpointScenarioContext(
      scenario = scenario,
      tableName = tableName,
      preCheckpointSnapshot = preCheckpointSnapshot,
      manifestCommitVersion = manifestCommitVersion,
      checkpoint = checkpoint,
      provider = provider,
      postCheckpointSnapshot = postCheckpointSnapshot)
  }

  protected def commitCheckpoint(deltaLog: DeltaLog, incremental: Boolean): Unit = {
    val triggerName = if (incremental) {
      AMTTriggerMode.CheckpointIntervalIncremental.name
    } else {
      AMTTriggerMode.CheckpointIntervalFull.name
    }
    deltaLog.startTransaction().commit(
      Seq.empty,
      DeltaOperations.OptimizeCheckpoint(
        incremental = incremental,
        triggerName = triggerName))
  }

  private def assertAMTCheckpointScenarioInvariants(
      context: AMTCheckpointScenarioContext): Unit = {
    val scenario = context.scenario
    val checkpoint = context.checkpoint
    assert(checkpoint.contentRoot.isIncremental.contains(scenario.isIncremental),
      s"${scenario.name}: wrong incremental tag ${checkpoint.contentRoot.isIncremental}")
    val expectedLastFull = scenario match {
      case AMTCheckpointScenario.InlineIncremental |
          AMTCheckpointScenario.DeferredIncremental =>
        val bootstrap = amtProvider(context.preCheckpointSnapshot).getOrElse(
          fail("incremental scenario must bootstrap a full checkpoint"))
        bootstrap.checkpointAction.contentRoot.lastManifestCommitWithFullRewrite.get
      case AMTCheckpointScenario.DeferredFull => checkpoint.version
    }
    assert(checkpoint.contentRoot.lastManifestCommitWithFullRewrite.contains(expectedLastFull),
      s"${scenario.name}: wrong last-full marker " +
        checkpoint.contentRoot.lastManifestCommitWithFullRewrite)
    assert(context.provider.checkpointVersion == checkpoint.version)
    assert(context.provider.checkpointAction == checkpoint)
    assert(context.postCheckpointSnapshot.version == context.manifestCommitVersion)
    val actionsFromCheckpointedCommit =
      actionsAt(context.postCheckpointSnapshot.deltaLog, checkpoint.version)
    if (scenario.isInline) {
      assert(actionsFromCheckpointedCommit.count(_.isInstanceOf[Checkpoint]) == 1,
        "inline checkpoint must be carried by the business commit")
    } else {
      assert(!actionsFromCheckpointedCommit.exists(_.isInstanceOf[Checkpoint]),
        "deferred business commit must not carry a Checkpoint")
      val actionsFromManifestCommit =
        actionsAt(context.postCheckpointSnapshot.deltaLog, context.manifestCommitVersion)
      assert(!actionsFromManifestCommit.exists {
        case _: AddFile | _: RemoveFile => true
        case _ => false
      }, "deferred manifest commit must not carry business file actions")
    }
  }

  /**
   * Asserts the manifest tree round-trips the live file set exactly: reconstructing from the
   * checkpoint (root + leaves, minus MDV-masked entries and root tombstones) must yield precisely
   * `snapshot.allFiles`, with no entry dropped or duplicated.
   *
   * Call this from tests that are about the tree capturing table state. It is deliberately NOT run
   * for every scenario: it costs a full reconstruction scan per call, which is wasted on tests that
   * assert something else (field ids, log-segment trimming, back references).
   */
  protected def assertReconstructsLiveFileSet(context: AMTCheckpointScenarioContext): Unit = {
    val snapshot = context.postCheckpointSnapshot
    val committed = snapshot.allFiles.collect().map(_.path).toSet
      val reconstructed = context.provider
        .loadActionsForStateReconstruction(spark, snapshot.deltaLog)
        .getOrElse(fail(s"${context.scenario.name}: provider must contribute file actions."))
        .where(col("add").isNotNull)
        .select("add.path")
        .collect()
        .map(_.getString(0))
      assert(reconstructed.length == reconstructed.toSet.size,
        s"${context.scenario.name}: reconstruction must not duplicate entries; got " +
          s"${reconstructed.toSeq.diff(reconstructed.distinct.toSeq)}")
      assert(reconstructed.toSet == committed,
        s"${context.scenario.name}: file set changed: committed=$committed " +
          s"reconstructed=${reconstructed.toSet}")
  }

  /** True iff `name` looks like an AMT leaf parquet file. */
  protected def isLeafFileName(name: String): Boolean =
    name.startsWith("leaf-") && name.endsWith(".parquet")

  /** True iff `name` looks like an AMT root parquet file. */
  protected def isRootFileName(name: String): Boolean =
    name.startsWith("root-") && name.endsWith(".parquet")

  /** Lists the AMT manifest files under `<path>/metadata/` matching `predicate` on the name. */
  protected def metadataFiles(path: String, predicate: String => Boolean): Seq[File] = {
    val dir = new File(path, FileNames.AMT_METADATA_DIR_NAME)
    if (!dir.exists()) Seq.empty
    else Option(dir.listFiles()).toSeq.flatten.filter(f => predicate(f.getName))
  }

  protected def leafFiles(path: String): Seq[File] =
    metadataFiles(path, isLeafFileName)

  protected def rootFiles(path: String): Seq[File] =
    metadataFiles(path, isRootFileName)

  /** Returns the actions committed at exactly `version`. */
  protected def actionsAt(deltaLog: DeltaLog, version: Long): Seq[Action] =
    deltaLog.getChanges(version).find(_._1 == version).map(_._2).getOrElse(Seq.empty)

  /** The [[Checkpoint]] committed at exactly `version`, if any. */
  protected def checkpointAt(deltaLog: DeltaLog, version: Long): Option[Checkpoint] = {
    val checkpoints = actionsAt(deltaLog, version).collect { case c: Checkpoint => c }
    assert(checkpoints.size <= 1,
      s"A commit may carry at most one Checkpoint; v$version has ${checkpoints.size}: $checkpoints")
    checkpoints.headOption
  }

  /**
   * Total DATA (content_type=0) entry rows across the leaves reachable from the CURRENT snapshot's
   * manifest tree. Reads only the provider's leaves (not every file under `metadata/`, which
   * accumulates superseded leaves from earlier checkpoints).
   *
   * Note: this counts *physical* leaf entries and does NOT subtract Manifest Deletion Vectors, nor
   * does it count live files stored directly in the root. On an incremental tree a deleted file's
   * entry stays physically present in its carried-forward leaf (tombstoned via the leaf MDV), so
   * this can exceed the live file count. To assert the tree captures exactly the live file set,
   * call [[assertReconstructsLiveFileSet]] instead.
   */
  protected def currentLeafDataEntries(snapshot: Snapshot): Long = {
    val provider = amtProvider(snapshot)
      .getOrElse(fail("Snapshot has no AMTCheckpointProvider."))
      provider.leafManifestAbsolutePaths.map { leafPath =>
        spark.read.parquet(leafPath.toString)
          .where(col("content_type") === AMTSingleAction.ContentType.Type.Data)
          .count()
      }.sum
  }

}
