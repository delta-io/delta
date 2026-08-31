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

import scala.collection.immutable.ListMap

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions}
import org.apache.spark.sql.delta.{AdaptiveMetadataTableFeature, CommitStats, DeltaLog, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile, Checkpoint, RemoveFile}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType}

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

  /**
   * Runs `body` with the format check that rejects reading AMT manifest parquet files directly
   * disabled, so a test can read a root or leaf manifest off disk.
   */
  protected def allowReadWithinDeltaLog[T](body: => T): T = {
    body
  }

  /** Typed view of a snapshot's checkpoint provider when it is AMT-backed. */
  protected def amtProvider(snapshot: Snapshot): Option[AMTCheckpointProvider] =
    snapshot.checkpointProvider match {
      case amt: AMTCheckpointProvider => Some(amt)
      case _ => None
    }

  /**
   * Runs `body` on the DATA (content_type=0) entry rows across `paths`, read straight off disk
   * (pass a provider's manifest paths -- its root and/or live leaves). The read runs with the
   * path-based Delta format check disabled, and `body` runs inside that scope, so its terminal
   * action reads under the disabled check.
   */
  protected def withManifestDataEntries[T](paths: Seq[String])(body: DataFrame => T): T =
    allowReadWithinDeltaLog {
      body(
        spark.read.parquet(paths: _*)
          .where(col("content_type") === AMTSingleAction.ContentType.Type.Data))
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
      location: Option[String] = None,
      tableSchema: String = "id INT",
      partitionColumns: Seq[String] = Seq.empty): Unit = {
    val locationClause = location.map(l => s"LOCATION '$l'").getOrElse("")
    val partitionClause =
      if (partitionColumns.isEmpty) ""
      else s"PARTITIONED BY (${partitionColumns.mkString(", ")})"
    sql(
      s"""CREATE TABLE $tableName ($tableSchema) USING DELTA
         |$locationClause
         |$partitionClause
         |TBLPROPERTIES (
         |  '${propertyKey(AdaptiveMetadataTableFeature)}' = '$FEATURE_PROP_SUPPORTED',
         |  'delta.columnMapping.mode' = 'id',
         |  'delta.enableDeletionVectors' = 'true',
         |  'delta.checkpointInterval' = '$checkpointInterval')""".stripMargin)
  }

  /**
   * Appends `numFiles` separate data files to `tableName` in a single commit, each holding
   * `rowsPerFile` rows (so `numFiles * rowsPerFile` rows total, ids `startId` until
   * `startId + numFiles * rowsPerFile`). `startId` lets successive calls append disjoint id ranges.
   * `columnExprs` is the projection over `range`'s `id`, one expression per table column; a
   * partitioned table passes its partition columns here too. The rows are generated in a single
   * partition and capped at `rowsPerFile` per file, so exactly `numFiles` files are written with
   * deterministic names.
   */
  protected def appendRowsAsSeparateFiles(
      tableName: String,
      numFiles: Int,
      rowsPerFile: Int = 1,
      startId: Int = 0,
      columnExprs: Seq[String] = Seq("CAST(id AS INT)")): Unit = {
    require(numFiles > 0 && rowsPerFile > 0,
      s"numFiles ($numFiles) and rowsPerFile ($rowsPerFile) must be positive.")
    val numRows = numFiles * rowsPerFile
    require(numRows % rowsPerFile == 0,
      s"numRows ($numRows) must be divisible by rowsPerFile ($rowsPerFile).")
    withSQLConf(
        "spark.sql.files.maxRecordsPerFile" -> rowsPerFile.toString,
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false") {
      sql(
        s"""INSERT INTO $tableName
           |SELECT ${columnExprs.mkString(", ")}
           |FROM range($startId, ${startId + numRows}, 1, 1)""".stripMargin)
    }
  }

  /** All live [[AddFile]]s of `snapshot`. */
  protected def liveAddFiles(snapshot: Snapshot): Seq[AddFile] =
    snapshot.allFiles.collect().toSeq

  /**
   * Test column fixture
   */
  protected case class TestColumn(
      name: String,
      dataType: DataType,
      valueExpr: String,
      partitionable: Boolean) {
    /** This column as a `StructField`, its name prefixed (e.g. `p_`, `c_`, `d_`). */
    def structField(prefix: String): StructField = StructField(prefix + name, dataType)

    /** This column as a `<name> <type>` SQL column definition, its name prefixed. */
    def columnDef(prefix: String): String = s"$prefix$name ${dataType.sql}"
  }

  /**
   * Every type the AMT fixtures exercise, keyed by column [[TestColumn.name]].
   */
  protected val allTypeColumns: ListMap[String, TestColumn] = {
    val columns = Seq(
      TestColumn("int", IntegerType, "CAST(id AS INT)", partitionable = true),
      TestColumn("long", LongType, "CAST(id AS LONG)", partitionable = true),
      TestColumn("short", ShortType, "CAST(id AS SHORT)", partitionable = true),
      TestColumn("byte", ByteType, "CAST(id AS BYTE)", partitionable = true),
      TestColumn("str", StringType, "CONCAT('row', CAST(id AS STRING))", partitionable = true),
      TestColumn("date", DateType, "DATE '2026-07-25' + CAST(id AS INT)", partitionable = true),
      TestColumn(
        "ts",
        TimestampType,
        "TIMESTAMP '2026-07-25 01:02:03.456' + MAKE_INTERVAL(0, 0, 0, CAST(id AS INT))",
        partitionable = true),
      TestColumn(
        "ts_ntz",
        TimestampNTZType,
        "CAST(TIMESTAMP_NTZ '2026-07-25 01:02:03.456' + " +
          "MAKE_INTERVAL(0, 0, 0, CAST(id AS INT)) AS TIMESTAMP_NTZ)",
        partitionable = true),
      TestColumn("dec", DecimalType(9, 3), "CAST(id AS DECIMAL(9,3)) / 8", partitionable = true),
      TestColumn("bool", BooleanType, "CAST(id % 2 AS BOOLEAN)", partitionable = true),
      TestColumn("float", FloatType, "CAST(id AS FLOAT)", partitionable = true),
      TestColumn("double", DoubleType, "CAST(id AS DOUBLE)", partitionable = true),
      TestColumn(
        "binary",
        BinaryType,
        "CAST(CONCAT('b', CAST(id AS STRING)) AS BINARY)",
        partitionable = true),
      TestColumn(
        "arr",
        ArrayType(StringType),
        "array(CONCAT('a', CAST(id AS STRING)))",
        partitionable = false),
      TestColumn(
        "nested",
        StructType(Seq(StructField("inner", IntegerType))),
        "named_struct('inner', CAST(id AS INT))",
        partitionable = false))
    ListMap(columns.map(c => c.name -> c): _*)
  }

  protected def partitionableTestColumns: Seq[TestColumn] =
    allTypeColumns.values.filter(_.partitionable).toSeq

  protected def allTypeColumnDefinitions: Seq[String] =
    allTypeColumns.values.map(_.columnDef("c_")).toSeq

  protected def allTypeColumnExprs: Seq[String] =
    allTypeColumns.values.map(_.valueExpr).toSeq

  /**
   * Creates an AMT table exercising every partition type (the `p_`-prefixed
   * [[partitionableTestColumns]]) alongside the `d_`-prefixed data columns that content stats are
   * collected on. Appends `numFiles` single-row files (one distinct value per row per column), and
   * runs `body` with the table's [[DeltaLog]].
   */
  protected def withAllTypesTable(
      tableName: String,
      numFiles: Int,
      maxEntriesPerLeaf: Int = entriesPerLeaf)(body: DeltaLog => Unit): Unit = {
    // Every type, so the content-stats goldens cover the full set of data columns.
    val dataColumns: Seq[TestColumn] = allTypeColumns.values.toSeq
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> maxEntriesPerLeaf.toString) {
      withTable(tableName) {
        createAMTTable(
          tableName,
          checkpointInterval = Int.MaxValue,
          tableSchema =
            (dataColumns.map(_.columnDef("d_")) ++ partitionableTestColumns.map(_.columnDef("p_")))
              .mkString(", "),
          partitionColumns = partitionableTestColumns.map("p_" + _.name))
        if (numFiles > 0) {
          appendRowsAsSeparateFiles(
            tableName,
            numFiles = numFiles,
            columnExprs = (dataColumns ++ partitionableTestColumns).map(_.valueExpr))
        }
        body(deltaLogForName(tableName))
      }
    }
  }

  /**
   * The per-leaf cap tests use to get a deterministic, multi-entry leaf layout.
   *
   * A test that needs a leaf-resident entry cannot just write a file or two. For example, a back
   * reference or a manifest deletion vector both need an entry that actually lives in a leaf. But a
   * full rewrite spreads the live files over cap-sized partitions by hash of path and skips the
   * empty ones, and if there is only a single leaf produced, that leaf is promoted to be a root, so
   * a small table can end up with no leaf at all.
   */
  protected val entriesPerLeaf: Int = 10

  /**
   * File count that packs into whole [[entriesPerLeaf]]-sized leaves.
   */
  protected val leafPackedFiles: Int = 3 * entriesPerLeaf

  /** Session conf that puts [[entriesPerLeaf]] entries in each leaf. */
  protected def leafPackingConfs: Seq[(String, String)] =
    Seq(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> entriesPerLeaf.toString)

  /** Leaves that `numFiles` live files pack into at [[entriesPerLeaf]] entries per leaf. */
  protected def expectedLeafCount(numFiles: Int): Int =
    math.ceil(numFiles.toDouble / entriesPerLeaf).toInt

  /**
   * Asserts `leaves` holds exactly the leaves `numFiles` live files pack into. Fails with the
   * arithmetic spelled out, so a count mismatch reads as a packing problem rather than a bare
   * number comparison.
   */
  protected def assertLeafCount(
      leaves: Seq[DataManifestEntry], numFiles: Int = leafPackedFiles): Unit = {
    val expected = expectedLeafCount(numFiles)
    assert(leaves.size == expected,
      s"$numFiles files at $entriesPerLeaf per leaf must pack into $expected leaves; " +
        s"got ${leaves.size}.")
  }

  /** Asserts two leaf sequences are field-by-field equal. */
  protected def assertLeavesEqual(
      actual: Seq[DataManifestEntry], expected: Seq[DataManifestEntry]): Unit =
    AMTLeafComparisons.assertLeavesEqual(actual, expected)

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
   * @param postSetupSnapshot the snapshot after `setup`
   * @param preCheckpointSnapshot the snapshot immediately before the checkpoint is constructed: for
   *                              deferred scenarios, after the trigger's data commit but before the
   *                              checkpoint commit; for inline scenarios, the same as
   *                              `postSetupSnapshot`.
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
      postSetupSnapshot: Snapshot,
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
      sqlConfs: Seq[(String, String)] = Seq.empty,
      tableSchema: String = "id INT",
      partitionColumns: Seq[String] = Seq.empty)(
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
            createAMTTable(
              scenarioTable,
              checkpointInterval = Int.MaxValue,
              tableSchema = tableSchema,
              partitionColumns = partitionColumns)
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
    val postSetupSnapshot = deltaLog.update()

    def runCheckpointTrigger(): Unit = {
      inlineCheckpointTriggerActionsOrSQL.map(_(tableName)).foreach {
        case Left((actions, operation)) =>
          val txn = deltaLog.startTransaction()
          txn.commit(actions, operation)
        case Right(sqlText) => spark.sql(sqlText)
      }
    }

    // The snapshot immediately before the checkpoint is constructed. For deferred scenarios this is
    // taken after the trigger's data commit but before the checkpoint commit, so it can be compared
    // against the post-checkpoint snapshot. Inline scenarios write the checkpoint in the same
    // commit that adds data, so there is no distinct pre-construction state; it stays the
    // pre-trigger snapshot (and callers skip the before/after comparison for inline).
    val preCheckpointSnapshot: Snapshot = scenario match {
      case InlineIncremental =>
        withSQLConf(
            DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key
              -> "1") {
          runCheckpointTrigger()
        }
        postSetupSnapshot
      case _ =>
        runCheckpointTrigger()
        val beforeConstruction = deltaLog.update()
        commitCheckpoint(deltaLog, incremental = scenario.isIncremental)
        beforeConstruction
    }

    val checkpointedVersion = postSetupSnapshot.version +
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
      postSetupSnapshot = postSetupSnapshot,
      preCheckpointSnapshot = preCheckpointSnapshot,
      manifestCommitVersion = manifestCommitVersion,
      checkpoint = checkpoint,
      provider = provider,
      postCheckpointSnapshot = postCheckpointSnapshot)
  }

  /**
   * Emits an AMT checkpoint on `deltaLog` via the real commit path. `incremental = false` forces a
   * full rewrite; `true` an incremental one.
   */
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

  /**
   * Emits an incremental AMT checkpoint on `deltaLog` and returns the incremental write's shape
   * metrics, read back out of the logged [[CommitStats]] (None if no incremental metrics were
   * logged). Wraps the commit in usage tracking, whose buffer is process-wide, so this must not run
   * concurrently with another tracked commit.
   */
  protected def commitIncrementalCheckpointAndReturnMetrics(
      deltaLog: DeltaLog): Option[IncrementalAMTWriteMetrics] = {
    val attemptVersion = deltaLog.update().version + 1
    trackIncrementalAMTWriteMetrics(attemptVersion) {
      commitCheckpoint(deltaLog, incremental = true)
    }
  }

  /**
   * Runs `commit` and returns the [[IncrementalAMTWriteMetrics]] logged for the commit at
   * `commitVersion`, or None if that commit wrote a full (non-incremental) AMT. Exposed for tests
   * that commit user actions inline (their own checkpoint rides in the same commit) rather than
   * through [[commitCheckpoint]]'s empty OPTIMIZE CHECKPOINT.
   */
  protected def trackIncrementalAMTWriteMetrics(
      commitVersion: Long)(commit: => Unit): Option[IncrementalAMTWriteMetrics] = {
    Log4jUsageLogger.track {
      commit
    }.filter(e => e.metric == MetricDefinitions.EVENT_TAHOE.name &&
        e.tags.get("opType").contains("delta.commit.stats"))
      .map(e => JsonUtils.fromJson[CommitStats](e.blob))
      .find(_.commitVersion == commitVersion)
      .flatMap(_.amtWriteMetrics)
      .flatMap(_.attempts.headOption)
      .flatMap(_.incrementalWriteMetrics)
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
        val bootstrap = amtProvider(context.postSetupSnapshot).getOrElse(
          fail("incremental scenario must bootstrap a full checkpoint"))
        bootstrap.checkpointAction.contentRoot.lastManifestCommitWithFullRewrite.get
      case AMTCheckpointScenario.DeferredFull => checkpoint.version
    }
    assert(checkpoint.contentRoot.lastManifestCommitWithFullRewrite.contains(expectedLastFull),
      s"${scenario.name}: wrong last-full marker " +
        checkpoint.contentRoot.lastManifestCommitWithFullRewrite)
    assert(checkpoint.contentRoot.version == checkpoint.version,
      s"${scenario.name}: contentRoot.version ${checkpoint.contentRoot.version} must equal " +
        s"checkpoint.version ${checkpoint.version}")
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

  /** Forces every write to inline its AMT incrementally (a low action-count threshold). */
  protected def withInline[T](body: => T): T =
    withSQLConf(
      DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key -> "1") {
      body
    }

  /**
   * Runs the test with inline writes forced (a low action-count threshold).
   * AMT checkpoints will be emitted in every commit after the first full OPTIMIZE CHECKPOINT.
   */
  protected def testInline(testName: String)(body: => Unit): Unit = {
    test(s"$testName (inline)") {
      withInline { body }
    }
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
  protected def leafLiveDataEntryCount(snapshot: Snapshot): Long =
    leafLiveDataEntryCount(amtProvider(snapshot)
      .getOrElse(fail("Snapshot has no AMTCheckpointProvider.")))

  /**
   * Total DATA (content_type=0) entry rows across `provider`'s live leaves, 0 when the tree is
   * leafless. Like the [[Snapshot]]-based overload but driven by a provider a test built directly
   * (e.g. from a checkpoint), where no owning [[Snapshot]] exists.
   */
  protected def leafLiveDataEntryCount(provider: AMTCheckpointProvider): Long = {
    val leaves = provider.liveLeafManifestAbsolutePaths.map(_.toString)
    if (leaves.isEmpty) 0L else withManifestDataEntries(leaves)(_.count())
  }


  /**
   * The number of live files the CURRENT snapshot's AMT reconstructs across the WHOLE tree -- root
   * and leaves. Goes through the provider's own reconstruction, which drops MDV-masked leaf entries
   * and `tracking=removed` root tombstones, so it equals `snapshot.allFiles.count()` on both full
   * and incremental trees. Unlike [[leafLiveDataEntryCount]], it counts live files stored directly
   * in the root too (as an incremental commit does below the spill threshold).
   *
   * Prefer [[assertReconstructsLiveFileSet]] when the test runs through
   * [[testAcrossAMTCheckpointScenarios]]; this count is for scenario-specific tests that drive the
   * checkpoint themselves and so have no [[AMTCheckpointScenarioContext]].
   */
  protected def currentLiveDataEntries(snapshot: Snapshot): Long = {
    val provider = amtProvider(snapshot)
      .getOrElse(fail("Snapshot has no AMTCheckpointProvider."))
    provider.loadActionsForStateReconstruction(spark, snapshot.deltaLog)
      .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
      .where(col("add").isNotNull)
      .count()
  }

}

/**
 * Field-by-field equality for AMT leaf pointers, shared across suites (some of which do not extend
 * [[AMTCheckpointTestBase]]). Compares by content rather than case-class `==` (which compares the
 * `Array[Byte]` fields by reference) and rather than a json comparison (tests of serialization must
 * not rely on serialization to check their result).
 */
object AMTLeafComparisons {
  /** Value equality for `Option[Array[Byte]]` (case-class `==` compares arrays by reference). */
  private def sameBytes(a: Option[Array[Byte]], b: Option[Array[Byte]]): Boolean =
    (a, b) match {
      case (Some(x), Some(y)) => x.sameElements(y)
      case (None, None) => true
      case _ => false
    }

  /** Asserts two leaf pointers are field-by-field equal, comparing byte-array fields by content. */
  def assertLeafEquals(actual: DataManifestEntry, expected: DataManifestEntry): Unit = {
    // Blank the array-bearing members so a single `==` covers every other field structurally.
    def blanked(e: DataManifestEntry): DataManifestEntry = e.copy(
      tracking = e.tracking.copy(deleted_positions = None, replaced_positions = None),
      manifest_info = e.manifest_info.copy(dv = None),
      content_stats = None,
      key_metadata = None)
    assert(blanked(actual) == blanked(expected),
      s"leaf non-array fields differ: $actual vs $expected")
    assert(sameBytes(actual.tracking.deleted_positions, expected.tracking.deleted_positions),
      "tracking.deleted_positions differ")
    assert(sameBytes(actual.tracking.replaced_positions, expected.tracking.replaced_positions),
      "tracking.replaced_positions differ")
    assert(sameBytes(actual.manifest_info.dv, expected.manifest_info.dv), "manifest_info.dv differ")
    assert(sameBytes(actual.key_metadata, expected.key_metadata), "key_metadata differ")
    assert(actual.content_stats == expected.content_stats, "content_stats differ")
  }

  /** Asserts two leaf sequences are field-by-field equal (see [[assertLeafEquals]]). */
  def assertLeavesEqual(
      actual: Seq[DataManifestEntry], expected: Seq[DataManifestEntry]): Unit = {
    assert(actual.size == expected.size, s"leaf count differs: ${actual.size} vs ${expected.size}")
    actual.zip(expected).foreach { case (a, e) => assertLeafEquals(a, e) }
  }
}
