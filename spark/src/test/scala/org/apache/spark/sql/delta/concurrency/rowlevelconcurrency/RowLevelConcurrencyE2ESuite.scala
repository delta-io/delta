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

package org.apache.spark.sql.delta.concurrency.rowlevelconcurrency

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.GeneratedAsIdentityType.GeneratedByDefault
import org.apache.spark.sql.delta.actions.{Action, CommitInfo}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames
import io.delta.exceptions.DeltaConcurrentModificationException

import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * End-to-end concurrent-DML tests for the  Row-Level Concurrency (RLC) feature.
 *
 * These tests exercise the full pipeline from SQL DELETE/UPDATE/MERGE statements through
 * the [[ConflictChecker.checkRowLevelConflicts]] integration, complementing the unit-level
 * suites that call `tryRebase` directly on synthetic Action sequences.
 *
 * Each scenario uses [[ConflictResolutionTestUtils]]:
 *   1. Create an RLC-eligible table (DV-mode + Row Tracking + unpartitioned) with all rows
 *      packed into a single physical file -- the prerequisite for same-file RLC rebase.
 *   2. Start txnA and pause at the precommit barrier.
 *   3. Run txnB synchronously to completion. txnB becomes the "winning commit" against
 *      which txnA must conflict-check.
 *   4. Commit txnA. The result tells us whether RLC successfully rebased the conflict.
 *   5. Read back the table and assert the final state equals "winner then loser applied".
 *
 * Notes on test conventions used here:
 *  - Auto-compaction and optimize-write are explicitly disabled so the file shape is
 *    predictable. We assert a single AddFile before each scenario.
 *  - The `Delete`/`Update`/`Merge` helpers in [[ConflictResolutionTestUtils]] issue real
 *    SQL with WHERE clauses on `idCol`. With row tracking, each unique idCol corresponds
 *    to a stable row index in the file, so disjoint idCol sets always produce disjoint DV
 *    deltas (P4).
 *  - The legacy Merge helper uses `USING $tableName s` which can trigger a self-read
 *    ConcurrentDeleteReadException unrelated to RLC. We define a local
 *    [[InlineSourceMerge]] helper that USES a small inline VALUES source for testing the
 *    MERGE path of RLC.
 */
class RowLevelConcurrencyE2ESuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with ConflictResolutionTestUtils {

  import testImplicits._

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(DeltaSQLConf.DELTA_IDENTITY_COLUMN_ENABLED.key, "true")

  // ---------- helpers ----------

  /**
   * Confs that produce a deterministic single-file table shape for these tests. We turn
   * off auto-compaction / optimize-write so the initial INSERT lands in a single file and
   * subsequent DV-mode DML keeps the file count stable. Row Tracking and DV creation
   * defaults are inherited from 's defaults but set explicitly here for clarity.
   */
  private def baseTableConfs: Seq[(String, String)] = Seq(
    DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey -> "true",
    DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true",
    DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "false",
    DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false")

  /**
   * Build an RLC-eligible Delta table at `path`:
   *  - unpartitioned (RLC eligibility predicate rejects partitioned tables)
   *  - row tracking enabled
   *  - DV-mode writes enabled
   *  - exactly one physical AddFile (we coalesce to 1 partition and assert this)
   */
  private def createSingleFileTable(path: String, rows: Seq[Long]): DeltaLog = {
    withSQLConf(baseTableConfs: _*) {
      rows.toDF(ID_COLUMN)
        .coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)
    }
    val deltaLog = DeltaLog.forTable(spark, path)
    val activeFiles = deltaLog.update().allFiles.collect()
    assert(activeFiles.length == 1,
      s"Expected exactly one AddFile after setup, got ${activeFiles.length}")
    assert(RowLevelConcurrency.isSnapshotEligible(spark, deltaLog.update()),
      "Table must be RLC-eligible for E2E test")
    deltaLog
  }

  private def createIdentitySingleFileTable(path: String, rows: Seq[Long]): DeltaLog = {
    io.delta.tables.DeltaTable.create(spark)
      .location(path)
      .addColumn(IdentityColumnSpec(
        GeneratedByDefault,
        colName = ID_COLUMN).structField(spark))
      .addColumn(TestColumnSpec("value", LongType).structField(spark))
      .property(DeltaConfigs.ROW_TRACKING_ENABLED.key, "true")
      .property(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "true")
      .execute()
    withSQLConf(baseTableConfs: _*) {
      rows.map(id => (id, id)).toDF(ID_COLUMN, "value")
        .coalesce(1)
        .write
        .format("delta")
        .mode("append")
        .save(path)
    }
    val deltaLog = DeltaLog.forTable(spark, path)
    assert(deltaLog.update().allFiles.collect().length == 1)
    assert(!RowLevelConcurrency.isSnapshotEligible(spark, deltaLog.update()),
      "Identity-column tables must be ineligible for RLC")
    deltaLog
  }

  /** Same as [[createSingleFileTable]] but builds a partitioned table for the
   * RLC-skipped regression test. */
  private def createPartitionedTable(path: String, rows: Seq[Long]): DeltaLog = {
    withSQLConf(baseTableConfs: _*) {
      rows.toDF(ID_COLUMN)
        .withColumn(PARTITION_COLUMN, lit(0L))
        .coalesce(1)
        .write
        .format("delta")
        .partitionBy(PARTITION_COLUMN)
        .mode("overwrite")
        .save(path)
    }
    DeltaLog.forTable(spark, path)
  }

  /** Same as [[createSingleFileTable]] but builds a table WITHOUT Deletion Vectors and
   *  WITHOUT Row Tracking enabled, so RLC eligibility rejects the snapshot and the
   *  concurrent-write hint helper should fire. */
  private def createNonRlcEligibleTable(path: String, rows: Seq[Long]): DeltaLog = {
    withSQLConf(
      DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "false",
      DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false") {
      rows.toDF(ID_COLUMN)
        .coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)
    }
    val deltaLog = DeltaLog.forTable(spark, path)
    assert(!RowLevelConcurrency.isSnapshotEligible(spark, deltaLog.update()),
      "Helper precondition: table must NOT be RLC-eligible")
    deltaLog
  }

  /**
   * Read the latest CommitInfo (the most recent commit version) for assertions on
   * isolationLevel and operation type.
   */
  private def latestCommitInfo(deltaLog: DeltaLog): CommitInfo = {
    val version = deltaLog.update().version
    val commitFile = FileNames.unsafeDeltaFile(deltaLog.logPath, version)
    val commitActions = deltaLog.store.read(commitFile, deltaLog.newDeltaHadoopConf())
      .map(Action.fromJson)
    commitActions.collectFirst { case ci: CommitInfo => ci }
      .getOrElse(throw new IllegalStateException(s"No CommitInfo in version $version"))
  }

  /** Read all rows from the table and return idCol values sorted. */
  private def selectAllIds(path: String): Seq[Long] = {
    spark.read.format("delta").load(path)
      .select(ID_COLUMN)
      .as[Long]
      .collect()
      .sorted
      .toSeq
  }

  /**
   * Custom MERGE helper that uses an inline VALUES source (not a self-read on the target
   * table) so it never trips the source-side ConcurrentDeleteReadException unrelated to
   * RLC behavior under test.
   *
   * Also overrides [[executeImpl]] to qualify the path with `file:`, since the default
   * implementation in [[ConflictResolutionTestUtils]] strips the scheme via
   * `dataPath.toUri.getPath`, causing path resolution to fall through to the cluster's
   * default FS (Fabric ABFSS in this dev environment).
   */
  case class InlineSourceMerge(
      deleteRows: Seq[Long],
      sqlConf: Map[String, String] = Map.empty) extends TestTransaction(sqlConf) {
    override val name: String =
      s"INLINE_MERGE(${abbreviate(deleteRows.mkString(","), "...", 10)})($sqlConfStr)"

    override def toSQL(tableName: String): String = {
      val values = deleteRows.map(r => s"($r)").mkString(", ")
      s"""
         |MERGE INTO $tableName t
         |USING (SELECT * FROM (VALUES $values) AS src(${ID_COLUMN})) s
         |ON t.${ID_COLUMN} = s.${ID_COLUMN}
         |WHEN MATCHED THEN DELETE
         |""".stripMargin
    }

    override def executeImpl(ctx: TestContext): Unit = {
      spark.sql(toSQL(schemeQualifiedDeltaPath(ctx))).collect()
    }

    override def dataChange: Boolean = true
  }

  /**
   * Mixed MERGE with all three clauses: `WHEN MATCHED THEN DELETE` for `deleteIds`,
   * `WHEN MATCHED THEN UPDATE` for `updateIds` (re-writes them to a sentinel value),
   * and `WHEN NOT MATCHED THEN INSERT` for `insertIds`. Used to verify that RLC masks
   * only the same-path AddFile/RemoveFile pair it actually rebased, while the winner's
   * new-path outputs stay subject to normal Serializable conflict detection.
   */
  case class InlineMixedMerge(
      deleteIds: Seq[Long],
      updateIds: Seq[Long],
      insertIds: Seq[Long],
      sqlConf: Map[String, String] = Map.empty) extends TestTransaction(sqlConf) {
    override val name: String =
      s"INLINE_MIXED_MERGE(d=${deleteIds.mkString(",")};" +
        s"u=${updateIds.mkString(",")};i=${insertIds.mkString(",")})($sqlConfStr)"

    override def toSQL(tableName: String): String = {
      // Source rows: union of all matched IDs (for DELETE+UPDATE) and the not-matched
      // INSERT IDs. We distinguish UPDATE from DELETE via a side column `op`.
      val rows = deleteIds.map(id => s"($id, 'd')") ++
        updateIds.map(id => s"($id, 'u')") ++
        insertIds.map(id => s"($id, 'i')")
      val values = rows.mkString(", ")
      s"""
         |MERGE INTO $tableName t
         |USING (SELECT * FROM (VALUES $values) AS src(${ID_COLUMN}, op)) s
         |ON t.${ID_COLUMN} = s.${ID_COLUMN}
         |WHEN MATCHED AND s.op = 'd' THEN DELETE
         |WHEN MATCHED AND s.op = 'u' THEN UPDATE SET ${ID_COLUMN} = -t.${ID_COLUMN}
         |WHEN NOT MATCHED AND s.op = 'i' THEN INSERT (${ID_COLUMN}) VALUES (s.${ID_COLUMN})
         |""".stripMargin
    }

    override def executeImpl(ctx: TestContext): Unit = {
      spark.sql(toSQL(schemeQualifiedDeltaPath(ctx))).collect()
    }

    override def dataChange: Boolean = true
  }


  /** Returns the table reference `delta.\`<scheme-qualified-path>\`` for SQL. The default
   * `dataPath.toUri.getPath` strips the scheme, which breaks path resolution when the
   * cluster default FS is not local. */
  private def schemeQualifiedDeltaPath(ctx: TestContext): String =
    s"delta.`${ctx.deltaLog.dataPath.toString}`"

  /** A DELETE transaction that targets the table by its scheme-qualified path (see
   * [[schemeQualifiedDeltaPath]]). Behaves identically to the base [[Delete]] otherwise. */
  case class DeleteE2E(
      rows: Seq[Long],
      sqlConf: Map[String, String] = Map.empty) extends TestTransaction(sqlConf) {
    override val name: String =
      s"DELETE_E2E(${abbreviate(rows.mkString(","), "...", 10)})($sqlConfStr)"

    override def toSQL(tableName: String): String = {
      val inRowsStr = rows.mkString("(", ", ", ")")
      s"DELETE FROM $tableName WHERE $ID_COLUMN IN $inRowsStr"
    }

    override def executeImpl(ctx: TestContext): Unit = {
      spark.sql(toSQL(schemeQualifiedDeltaPath(ctx))).collect()
    }

    override def dataChange: Boolean = true
  }

  /** An UPDATE transaction that targets the table by its scheme-qualified path. */
  case class UpdateE2E(
      rows: Seq[Long],
      setValue: Long = 42,
      sqlConf: Map[String, String] = Map.empty) extends TestTransaction(sqlConf) {
    override val name: String =
      s"UPDATE_E2E(${abbreviate(rows.mkString(","), "...", 10)})($sqlConfStr)"

    override def toSQL(tableName: String): String = {
      val inRowsStr = rows.mkString("(", ", ", ")")
      s"UPDATE $tableName SET $ID_COLUMN=$setValue WHERE $ID_COLUMN IN $inRowsStr"
    }

    override def executeImpl(ctx: TestContext): Unit = {
      spark.sql(toSQL(schemeQualifiedDeltaPath(ctx))).collect()
    }

    override def dataChange: Boolean = true
  }

  /** Common assertion that the rebase telemetry / fall-through happened as expected by
   * comparing committed transaction count: with RLC, both txns should commit. */
  private def assertBothCommitted(ctx: TestContext, expectedCount: Int): Unit = {
    val committed = ctx.getCommittedTransactions
    assert(committed.size == expectedCount,
      s"Expected $expectedCount committed transactions, got ${committed.size}: " +
        s"${committed.map(_.name).mkString("[", ",", "]")}")
  }

  private def assertOneActiveLogicalVersionPerPath(deltaLog: DeltaLog): Unit = {
    val duplicatePaths = deltaLog.update().allFiles.collect().groupBy(_.path).collect {
      case (path, files) if files.length != 1 => path -> files.length
    }
    assert(duplicatePaths.isEmpty,
      s"Expected one active logical version per physical path, found: $duplicatePaths")
  }

  /** Run `loser` to the precommit barrier, then run `winner` to completion, then commit
   * `loser`. After this, `loser` has been conflict-checked against `winner`. Returns the
   * outcome (either a successful commit or a SparkException with the underlying cause). */
  private def runWinnerThenLoser(
      ctx: TestContext,
      loser: TestTransaction,
      winner: TestTransaction): Either[Throwable, Unit] = {
    try {
      loser.interleave(ctx) {
        winner.execute(ctx)
      }
      Right(())
    } catch {
      case e: SparkException => Left(if (e.getCause != null) e.getCause else e)
      case e: Throwable => Left(e)
    }
  }

  // ---------- Scenario 1: DELETE + DELETE disjoint, same file ----------

  test("RLC E2E: DELETE + DELETE on disjoint rows of the same file both commit") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createSingleFileTable(path, (0L until 100L).toSeq)
      val ctx = new TestContext(deltaLog)

      val loser = DeleteE2E(rows = Seq(10L))
      val winner = DeleteE2E(rows = Seq(20L))

      val outcome = runWinnerThenLoser(ctx, loser = loser, winner = winner)
      assert(outcome.isRight,
        s"Loser commit should succeed via RLC, but failed: ${outcome.left.getOrElse("")}")

      assertBothCommitted(ctx, expectedCount = 2)

      val remaining = selectAllIds(path)
      val expected = (0L until 100L).filterNot(Set(10L, 20L).contains).toSeq
      assert(remaining == expected,
        s"Final state should reflect both deletes; missing/extra: " +
          s"${expected.diff(remaining)} vs ${remaining.diff(expected)}")
      assertOneActiveLogicalVersionPerPath(deltaLog)
    }
  }

  // ---------- Scenario 2: DELETE + DELETE overlapping rows ----------

  test("RLC E2E: DELETE + DELETE on overlapping rows -> loser aborts") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createSingleFileTable(path, (0L until 100L).toSeq)
      val ctx = new TestContext(deltaLog)

      val loser = DeleteE2E(rows = Seq(10L, 20L))
      val winner = DeleteE2E(rows = Seq(20L, 30L))

      val outcome = runWinnerThenLoser(ctx, loser = loser, winner = winner)
      assert(outcome.isLeft,
        s"Loser must abort on overlapping deletes (P4 fails); got success")
      val cause = outcome.left.get
      assert(cause.isInstanceOf[DeltaConcurrentModificationException],
        s"Expected DeltaConcurrentModificationException, got: " +
          s"${cause.getClass.getName}: ${cause.getMessage}")

      assertBothCommitted(ctx, expectedCount = 1)
    }
  }

  // ---------- Scenario 3: UPDATE + UPDATE disjoint ----------

  test("RLC E2E: UPDATE winner post-image falls back to Serializable conflict detection") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createSingleFileTable(path, (0L until 100L).toSeq)
      val ctx = new TestContext(deltaLog)

      val loser = UpdateE2E(rows = Seq(10L), setValue = 200L)
      val winner = UpdateE2E(rows = Seq(20L), setValue = 300L)

      val outcome = runWinnerThenLoser(ctx, loser = loser, winner = winner)
      assert(outcome.isLeft,
        "A new-path UPDATE post-image has no action-level lineage and must remain conflicting")
      assert(outcome.left.get.isInstanceOf[DeltaConcurrentModificationException])
      assertBothCommitted(ctx, expectedCount = 1)

      val remaining = selectAllIds(path)
      val expected = ((0L until 100L).filterNot(_ == 20L) :+ 300L).sorted
      assert(remaining == expected,
        s"Only the winning UPDATE should be visible; got $remaining")
    }
  }

  // ---------- Scenario 4: UPDATE + DELETE disjoint ----------

  test("RLC E2E: UPDATE + DELETE on disjoint rows of the same file both commit") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createSingleFileTable(path, (0L until 100L).toSeq)
      val ctx = new TestContext(deltaLog)

      val loser = UpdateE2E(rows = Seq(10L), setValue = 999L)
      val winner = DeleteE2E(rows = Seq(20L))

      val outcome = runWinnerThenLoser(ctx, loser = loser, winner = winner)
      assert(outcome.isRight,
        s"Loser commit should succeed via RLC, but failed: ${outcome.left.getOrElse("")}")
      assertBothCommitted(ctx, expectedCount = 2)

      val remaining = selectAllIds(path)
      val expected = ((0L until 100L).filterNot(Set(10L, 20L).contains) ++ Seq(999L)).sorted
      assert(remaining == expected, s"Final state mismatch; got $remaining")
    }
  }

  // ---------- Scenario 5: MERGE (delete-only) + DELETE disjoint ----------

  test("RLC E2E: MERGE-delete + DELETE on disjoint rows of the same file both commit") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createSingleFileTable(path, (0L until 100L).toSeq)
      val ctx = new TestContext(deltaLog)

      val loser = InlineSourceMerge(deleteRows = Seq(10L))
      val winner = DeleteE2E(rows = Seq(20L))

      val outcome = runWinnerThenLoser(ctx, loser = loser, winner = winner)
      assert(outcome.isRight,
        s"Loser MERGE should succeed via RLC, but failed: ${outcome.left.getOrElse("")}")
      assertBothCommitted(ctx, expectedCount = 2)

      val remaining = selectAllIds(path)
      val expected = (0L until 100L).filterNot(Set(10L, 20L).contains).toSeq
      assert(remaining == expected, s"Final state mismatch; got $remaining")
    }
  }

  // ---------- Scenario 6: RLC disabled regression guard ----------

  test("RLC E2E: with RLC disabled, disjoint DELETE+DELETE still aborts loser") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createSingleFileTable(path, (0L until 100L).toSeq)
      val ctx = new TestContext(deltaLog)

      val rlcOff = Map(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "false")
      val loser = DeleteE2E(rows = Seq(10L), sqlConf = rlcOff)
      val winner = DeleteE2E(rows = Seq(20L), sqlConf = rlcOff)

      val outcome = runWinnerThenLoser(ctx, loser = loser, winner = winner)
      assert(outcome.isLeft,
        s"With RLC disabled, loser must abort (regression guard)")
      val cause = outcome.left.get
      assert(cause.isInstanceOf[DeltaConcurrentModificationException],
        s"Expected DeltaConcurrentModificationException, got: " +
          s"${cause.getClass.getName}: ${cause.getMessage}")
    }
  }

  // ---------- Scenario 7: Partitioned table -> RLC skipped ----------

  test("RLC E2E: partitioned table -> RLC skipped, loser aborts") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createPartitionedTable(path, (0L until 100L).toSeq)
      assert(!RowLevelConcurrency.isSnapshotEligible(spark, deltaLog.update()),
        "Partitioned table must NOT be RLC-eligible")
      val ctx = new TestContext(deltaLog)

      val loser = DeleteE2E(rows = Seq(10L))
      val winner = DeleteE2E(rows = Seq(20L))

      val outcome = runWinnerThenLoser(ctx, loser = loser, winner = winner)
      assert(outcome.isLeft,
        "Loser must abort on partitioned tables (RLC eligibility rejects)")
    }
  }

  // ---------- Scenario 8: CommitInfo isolation level ----------

  test("RLC E2E: post-RLC CommitInfo records the table's isolation level") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createSingleFileTable(path, (0L until 100L).toSeq)
      val ctx = new TestContext(deltaLog)

      val loser = DeleteE2E(rows = Seq(10L))
      val winner = DeleteE2E(rows = Seq(20L))
      val outcome = runWinnerThenLoser(ctx, loser = loser, winner = winner)
      assert(outcome.isRight, "Sanity: RLC happy path should commit")

      val ci = latestCommitInfo(deltaLog)
      // The  fork only supports Serializable for the isolation-level config;
      // -DeltaConfigs.ISOLATION_LEVEL validator at DeltaConfig.scala:785-791
      // rejects WriteSerializable. So the recorded value is "Serializable".
      assert(ci.isolationLevel.contains("Serializable"),
        s"Expected isolationLevel = Some(Serializable*), got ${ci.isolationLevel}")
    }
  }

  // ---------- Scenario 9: Non-empty prior DV ----------

  test("RLC E2E: disjoint DELETE+DELETE with a non-empty prior DV both commit") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createSingleFileTable(path, (0L until 100L).toSeq)
      // Commit an initial DELETE so the file already carries a non-empty DV.
      withSQLConf(baseTableConfs: _*) {
        spark.sql(s"DELETE FROM delta.`$path` WHERE ${ID_COLUMN} = 5")
      }
      val activeFiles = deltaLog.update().allFiles.collect()
      assert(activeFiles.length == 1, "Initial DELETE should keep a single file (DV mode)")
      assert(activeFiles.head.deletionVector != null,
        "Initial DELETE should produce a non-empty deletion vector")

      val ctx = new TestContext(deltaLog)
      val loser = DeleteE2E(rows = Seq(10L))
      val winner = DeleteE2E(rows = Seq(20L))

      val outcome = runWinnerThenLoser(ctx, loser = loser, winner = winner)
      assert(outcome.isRight,
        s"RLC should rebase against a non-empty prior DV, but failed: " +
          s"${outcome.left.getOrElse("")}")
      assertBothCommitted(ctx, expectedCount = 2)

      val remaining = selectAllIds(path)
      val expected = (0L until 100L).filterNot(Set(5L, 10L, 20L).contains).toSeq
      assert(remaining == expected, s"Final state mismatch; got $remaining")
    }
  }

  // ---------- Scenario 10: Three-writer convergence (sequential rebase) ----------

  test("RLC E2E: three concurrent disjoint DELETEs all commit (multi-winner rebase)") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createSingleFileTable(path, (0L until 100L).toSeq)
      val ctx = new TestContext(deltaLog)

      // C is the last to commit and must rebase against A then B serially.
      // We start B and C first (paused at precommit), then commit them in order:
      // A first (no conflict), then B (rebases vs A), then C (rebases vs A, then B).
      val txnA = DeleteE2E(rows = Seq(10L))
      val txnB = DeleteE2E(rows = Seq(20L))
      val txnC = DeleteE2E(rows = Seq(30L))

      // Start B and C at the precommit barrier.
      txnB.start(ctx)
      txnC.start(ctx)
      // A: run to completion (winner #1).
      txnA.execute(ctx)
      // B: commit (winner #2; rebases against A).
      txnB.commit(ctx)
      // C: commit (rebases against A, then B).
      txnC.commit(ctx)

      assertBothCommitted(ctx, expectedCount = 3)
      assertOneActiveLogicalVersionPerPath(deltaLog)
      val remaining = selectAllIds(path)
      val expected = (0L until 100L).filterNot(Set(10L, 20L, 30L).contains).toSeq
      assert(remaining == expected, s"Final state mismatch; got $remaining")
    }
  }

  // ---------- Scenario 11: enablement hint surfaces in concurrent-write exceptions ----------

  test("RLC E2E: hint surfaces when a conflict aborts on a non-RLC-eligible table") {
    // When the RLC SQL conf is on (default) but the table is missing DVs/Row Tracking,
    // a real concurrent-write conflict must surface the enablement hint in the thrown
    // exception's message, naming the specific feature(s) to enable.
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createNonRlcEligibleTable(path, (0L until 100L).toSeq)
      val ctx = new TestContext(deltaLog)

      // Disjoint DELETEs on the same file. Without DV mode each DELETE rewrites
      // the file, so this fires the CoW conflict path -> ConcurrentDelete*Exception.
      val loser = DeleteE2E(rows = Seq(10L))
      val winner = DeleteE2E(rows = Seq(20L))

      val outcome = runWinnerThenLoser(ctx, loser = loser, winner = winner)
      assert(outcome.isLeft, "Loser must abort on a non-RLC-eligible table")
      val cause = outcome.left.get
      assert(cause.isInstanceOf[DeltaConcurrentModificationException],
        s"Expected DeltaConcurrentModificationException, got: " +
          s"${cause.getClass.getName}: ${cause.getMessage}")
      val msg = cause.getMessage
      assert(msg.contains("Row-Level Concurrency"),
        s"Hint should mention Row-Level Concurrency; got:\n$msg")
      assert(msg.contains("Deletion Vectors"),
        s"Hint should name Deletion Vectors as a missing feature; got:\n$msg")
      assert(msg.contains("Row Tracking"),
        s"Hint should name Row Tracking as a missing feature; got:\n$msg")
      assert(msg.contains("ALTER TABLE"),
        s"Hint should include an ALTER TABLE remediation example; got:\n$msg")
    }
  }

  // ---------- Scenario 12: mixed-MERGE winner (DELETE + UPDATE + INSERT clauses) ----------

  test("RLC E2E: Serializable mixed MERGE insert remains visible to predicate conflicts") {
    // The same-path DV pair can be rebased, but the winner's WHEN NOT MATCHED INSERT
    // output is unrelated to that path and must still pass through Serializable append
    // conflict detection.
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createSingleFileTable(path, (0L until 100L).toSeq)
      val ctx = new TestContext(deltaLog)

      // Loser deletes id=10. Winner's MERGE on the same file deletes id=20,
      // updates id=30 -> -30, and inserts a brand-new id=200 (new-path AddFile).
      val loser = DeleteE2E(rows = Seq(10L))
      val winner = InlineMixedMerge(
        deleteIds = Seq(20L),
        updateIds = Seq(30L),
        insertIds = Seq(200L))

      var outcome: Either[Throwable, Unit] = Right(())
      val usageLogs = Log4jUsageLogger.track {
        outcome = runWinnerThenLoser(ctx, loser = loser, winner = winner)
      }
      assert(outcome.isLeft,
        "Serializable conflict detection must see mixed-MERGE INSERT output")
      assert(outcome.left.get.isInstanceOf[DeltaConcurrentModificationException])
      assertBothCommitted(ctx, expectedCount = 1)
      val rlcEvents = usageLogs.flatMap(_.tags.get("opType"))
      assert(!rlcEvents.contains(RowLevelConcurrency.TELEMETRY_RESOLVED),
        s"Aborted conflict checking must not emit resolved telemetry: $rlcEvents")
      assert(rlcEvents.contains(RowLevelConcurrency.TELEMETRY_ABORTED_AFTER_REBASE),
        s"Expected final aborted-after-rebase telemetry, got: $rlcEvents")

      val remaining = selectAllIds(path)
      val expected = ((0L until 100L).filterNot(Set(20L, 30L).contains) :+ -30L :+ 200L)
        .sorted
      assert(remaining == expected, s"Final state mismatch; expected=$expected got=$remaining")
    }
  }

  test("RLC E2E: identity-column table falls back to default conflict detection") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      val deltaLog = createIdentitySingleFileTable(path, (0L until 100L).toSeq)
      val ctx = new TestContext(deltaLog)

      val outcome = runWinnerThenLoser(
        ctx,
        loser = DeleteE2E(rows = Seq(10L)),
        winner = DeleteE2E(rows = Seq(20L)))

      assert(outcome.isLeft,
        "Identity-column tables are unsupported by RLC and must use default conflicts")
      assert(outcome.left.get.isInstanceOf[DeltaConcurrentModificationException])
      assertBothCommitted(ctx, expectedCount = 1)
      assert(selectAllIds(path) == (0L until 100L).filterNot(_ == 20L))
    }
  }
}
