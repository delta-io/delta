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

package org.apache.spark.sql.delta.concurrency.rowlevelconcurrency

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.GeneratedAsIdentityType.GeneratedByDefault
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * Tests for the Row-Level Concurrency eligibility predicates defined in
 * [[RowLevelConcurrency]]. These predicates gate whether the RLC conflict
 * resolution phase runs inside [[ConflictChecker]].
 */
class RowLevelConcurrencyEligibilitySuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(DeltaSQLConf.DELTA_IDENTITY_COLUMN_ENABLED.key, "true")

  /**
   * Helper: create an unpartitioned Delta table with DVs + Row Tracking enabled.
   * Returns the table path.
   */
  private def createRlcEligibleTable(dir: java.io.File): String = {
    val path = "file:" + dir.getAbsolutePath
    withSQLConf(
      DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey -> "true",
      DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true"
    ) {
      spark.range(10).write.format("delta").save(path)
    }
    path
  }

  private def createIdentityTable(dir: java.io.File): String = {
    val path = "file:" + dir.getAbsolutePath
    io.delta.tables.DeltaTable.create(spark)
      .location(path)
      .addColumn(IdentityColumnSpec(
        GeneratedByDefault,
        colName = "idCol").structField(spark))
      .addColumn(TestColumnSpec("value", LongType).structField(spark))
      .property(DeltaConfigs.ROW_TRACKING_ENABLED.key, "true")
      .property(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "true")
      .execute()
    path
  }

  // ---------- Snapshot-level eligibility tests ----------

  test("eligible: unpartitioned table with DVs + RT enabled and RLC switch on") {
    withTempDir { dir =>
      val path = createRlcEligibleTable(dir)
      val snapshot = DeltaLog.forTable(spark, path).update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        assert(RowLevelConcurrency.isSnapshotEligible(spark, snapshot))
      }
    }
  }

  test("ineligible: RLC master switch is explicitly off") {
    withTempDir { dir =>
      val path = createRlcEligibleTable(dir)
      val snapshot = DeltaLog.forTable(spark, path).update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "false") {
        assert(!RowLevelConcurrency.isSnapshotEligible(spark, snapshot))
      }
    }
  }

  test("eligible (default-on): RLC master switch is on by default") {
    withTempDir { dir =>
      val path = createRlcEligibleTable(dir)
      val snapshot = DeltaLog.forTable(spark, path).update()
      // The master switch defaults to true, so no explicit conf is set here.
      assert(RowLevelConcurrency.isSnapshotEligible(spark, snapshot))
    }
  }

  test("ineligible: DVs not writable") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      withSQLConf(
        DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey -> "true",
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "false"
      ) {
        spark.range(10).write.format("delta").save(path)
      }
      val snapshot = DeltaLog.forTable(spark, path).update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        assert(!RowLevelConcurrency.isSnapshotEligible(spark, snapshot))
      }
    }
  }

  test("ineligible: Row Tracking not enabled") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      withSQLConf(
        DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey -> "false",
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true"
      ) {
        spark.range(10).write.format("delta").save(path)
      }
      val snapshot = DeltaLog.forTable(spark, path).update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        assert(!RowLevelConcurrency.isSnapshotEligible(spark, snapshot))
      }
    }
  }

  test("ineligible: partitioned table") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      withSQLConf(
        DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey -> "true",
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true"
      ) {
        spark.range(10).withColumn("part", org.apache.spark.sql.functions.lit(1))
          .write.format("delta").partitionBy("part").save(path)
      }
      val snapshot = DeltaLog.forTable(spark, path).update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        assert(!RowLevelConcurrency.isSnapshotEligible(spark, snapshot))
      }
    }
  }

  test("ineligible: identity-column table") {
    withTempDir { dir =>
      val snapshot = DeltaLog.forTable(spark, createIdentityTable(dir)).update()
      assert(ColumnWithDefaultExprUtils.hasIdentityColumn(snapshot.schema))
      assert(!RowLevelConcurrency.isSnapshotEligible(spark, snapshot))
      assert(!RowLevelConcurrency.isCommitEligible(spark, snapshot, Seq.empty))
    }
  }

  // ---------- Commit-level eligibility tests ----------

  test("commit eligible: no metadata mutation") {
    withTempDir { dir =>
      val path = createRlcEligibleTable(dir)
      val log = DeltaLog.forTable(spark, path)
      val snapshot = log.update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        val txnActions = Seq.empty[Action]
        assert(RowLevelConcurrency.isCommitEligible(spark, snapshot, txnActions))
      }
    }
  }

  test("commit ineligible: divergent metadata mutation") {
    withTempDir { dir =>
      val path = createRlcEligibleTable(dir)
      val log = DeltaLog.forTable(spark, path)
      val snapshot = log.update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        // Create a different metadata object
        val divergentMetadata = snapshot.metadata.copy(description = "changed")
        val txnActions: Seq[Action] = Seq(divergentMetadata)
        assert(!RowLevelConcurrency.isCommitEligible(spark, snapshot, txnActions))
      }
    }
  }

  test("commit eligible: byte-identical metadata is allowed") {
    withTempDir { dir =>
      val path = createRlcEligibleTable(dir)
      val log = DeltaLog.forTable(spark, path)
      val snapshot = log.update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        // Same metadata object as snapshot -- allowed (e.g., no-op metadata echo)
        val txnActions: Seq[Action] = Seq(snapshot.metadata)
        assert(RowLevelConcurrency.isCommitEligible(spark, snapshot, txnActions))
      }
    }
  }

  // ---------- SQL conf tests ----------

  test("RLC confs have expected defaults") {
    assert(spark.sessionState.conf.getConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED))
    assert(spark.sessionState.conf.getConf(
      DeltaSQLConf.ROW_LEVEL_CONCURRENCY_MAX_DV_BYTES_PER_FILE) == 1048576L)
    assert(spark.sessionState.conf.getConf(
      DeltaSQLConf.ROW_LEVEL_CONCURRENCY_MAX_DV_READS_PER_COMMIT) == 64)
    assert(spark.sessionState.conf.getConf(
      DeltaSQLConf.ROW_LEVEL_CONCURRENCY_MAX_RESOLUTION_TIME_MS) == 2000L)
  }

  test("RLC confs are settable") {
    withSQLConf(
      DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true",
      DeltaSQLConf.ROW_LEVEL_CONCURRENCY_MAX_DV_BYTES_PER_FILE.key -> "2097152",
      DeltaSQLConf.ROW_LEVEL_CONCURRENCY_MAX_DV_READS_PER_COMMIT.key -> "128",
      DeltaSQLConf.ROW_LEVEL_CONCURRENCY_MAX_RESOLUTION_TIME_MS.key -> "5000"
    ) {
      assert(spark.sessionState.conf.getConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED))
      assert(spark.sessionState.conf.getConf(
        DeltaSQLConf.ROW_LEVEL_CONCURRENCY_MAX_DV_BYTES_PER_FILE) == 2097152L)
      assert(spark.sessionState.conf.getConf(
        DeltaSQLConf.ROW_LEVEL_CONCURRENCY_MAX_DV_READS_PER_COMMIT) == 128)
      assert(spark.sessionState.conf.getConf(
        DeltaSQLConf.ROW_LEVEL_CONCURRENCY_MAX_RESOLUTION_TIME_MS) == 5000L)
    }
  }

  // ---------- persistent-DV defaults regression guard ----------

  test("persistent-DV defaults remain true (RLC P0 invariant)") {
    // RLC can only rebase DV-only modifications, so DML must default to writing persistent
    // deletion vectors. The per-op confs already default to true; this test guards against
    // an inadvertent regression that would silently disable RLC by switching DML back to
    // copy-on-write.
    assert(spark.sessionState.conf.getConf(
      DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS),
      "DELETE_USE_PERSISTENT_DELETION_VECTORS must remain true for RLC")
    assert(spark.sessionState.conf.getConf(
      DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS),
      "UPDATE_USE_PERSISTENT_DELETION_VECTORS must remain true for RLC")
    assert(spark.sessionState.conf.getConf(
      DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS),
      "MERGE_USE_PERSISTENT_DELETION_VECTORS must remain true for RLC")
  }

  // ---------- Telemetry constants ----------

  test("telemetry event names are well-formed") {
    assert(RowLevelConcurrency.TELEMETRY_RESOLVED.startsWith(
      "delta.conflictDetection.rowLevelConcurrency."))
    assert(RowLevelConcurrency.TELEMETRY_ABORTED_OVERLAP.startsWith(
      "delta.conflictDetection.rowLevelConcurrency."))
    assert(RowLevelConcurrency.TELEMETRY_ABORTED_BUDGET.startsWith(
      "delta.conflictDetection.rowLevelConcurrency."))
    assert(RowLevelConcurrency.TELEMETRY_ABORTED_SHAPE.startsWith(
      "delta.conflictDetection.rowLevelConcurrency."))
    assert(RowLevelConcurrency.TELEMETRY_ABORTED_DV_READ_FAILURE.startsWith(
      "delta.conflictDetection.rowLevelConcurrency."))
    assert(RowLevelConcurrency.TELEMETRY_ABORTED_DECODE_FAILURE.startsWith(
      "delta.conflictDetection.rowLevelConcurrency."))
    assert(RowLevelConcurrency.TELEMETRY_ABORTED_DV_WRITE_FAILURE.startsWith(
      "delta.conflictDetection.rowLevelConcurrency."))
    assert(RowLevelConcurrency.TELEMETRY_ABORTED_AFTER_REBASE.startsWith(
      "delta.conflictDetection.rowLevelConcurrency."))
    assert(RowLevelConcurrency.TELEMETRY_WOULD_RESOLVE.startsWith(
      "delta.conflictDetection.rowLevelConcurrency."))
  }

  // ---------- enablementHintIfMissing ----------

  test("hint: empty when RLC switch is off (regardless of features)") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      // table with NEITHER DVs nor RT
      spark.range(10).write.format("delta").save(path)
      val snapshot = DeltaLog.forTable(spark, path).update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "false") {
        val hint = RowLevelConcurrency.enablementHintIfMissing(
          snapshot, spark.sessionState.conf)
        assert(hint == "", s"Hint should be empty when RLC is disabled; got: $hint")
      }
    }
  }

  test("hint: empty when RLC switch is on and all prerequisites are met") {
    withTempDir { dir =>
      val path = createRlcEligibleTable(dir)
      val snapshot = DeltaLog.forTable(spark, path).update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        val hint = RowLevelConcurrency.enablementHintIfMissing(
          snapshot, spark.sessionState.conf)
        assert(hint == "",
          s"Hint should be empty when prerequisites are met; got: $hint")
      }
    }
  }

  test("hint: names both Deletion Vectors and Row Tracking when both are off") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      spark.range(10).write.format("delta").save(path)
      val snapshot = DeltaLog.forTable(spark, path).update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        val hint = RowLevelConcurrency.enablementHintIfMissing(
          snapshot, spark.sessionState.conf)
        assert(hint.contains("Row-Level Concurrency"), s"missing RLC mention; got: $hint")
        assert(hint.contains("Deletion Vectors"), s"missing DV mention; got: $hint")
        assert(hint.contains("Row Tracking"), s"missing RowTracking mention; got: $hint")
        assert(hint.contains(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key),
          s"missing DV property key; got: $hint")
        assert(hint.contains(DeltaConfigs.ROW_TRACKING_ENABLED.key),
          s"missing RT property key; got: $hint")
        assert(hint.contains("ALTER TABLE"), s"missing ALTER TABLE example; got: $hint")
        assert(hint.contains("if its row changes are disjoint"),
          s"hint must not assert row disjointness before conflict resolution; got: $hint")
      }
    }
  }

  test("hint: names only Deletion Vectors when RT is on but DVs are off") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      withSQLConf(
        DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey -> "true"
      ) {
        spark.range(10).write.format("delta").save(path)
      }
      val snapshot = DeltaLog.forTable(spark, path).update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        val hint = RowLevelConcurrency.enablementHintIfMissing(
          snapshot, spark.sessionState.conf)
        assert(hint.contains("Deletion Vectors"), s"missing DV mention; got: $hint")
        assert(!hint.contains("Row Tracking"),
          s"should NOT mention Row Tracking when it is enabled; got: $hint")
      }
    }
  }

  test("hint: names only Row Tracking when DVs are on but RT is off") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true"
      ) {
        spark.range(10).write.format("delta").save(path)
      }
      val snapshot = DeltaLog.forTable(spark, path).update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        val hint = RowLevelConcurrency.enablementHintIfMissing(
          snapshot, spark.sessionState.conf)
        assert(hint.contains("Row Tracking"), s"missing RT mention; got: $hint")
        assert(!hint.contains("Deletion Vectors"),
          s"should NOT mention Deletion Vectors when they are enabled; got: $hint")
      }
    }
  }

  test("hint: empty when table is partitioned (RLC unavailable, no actionable fix)") {
    withTempDir { dir =>
      val path = "file:" + dir.getAbsolutePath
      withSQLConf(
        DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey -> "true",
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true"
      ) {
        spark.range(10).withColumn("p", org.apache.spark.sql.functions.col("id").cast("int") % 3)
          .write.format("delta").partitionBy("p").save(path)
      }
      val snapshot = DeltaLog.forTable(spark, path).update()
      withSQLConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        val hint = RowLevelConcurrency.enablementHintIfMissing(
          snapshot, spark.sessionState.conf)
        // Partitioning is not a feature the user can toggle without rewriting the table,
        // so the hint is suppressed to avoid noise.
        assert(hint == "", s"Hint should be empty for partitioned tables; got: $hint")
      }
    }
  }

  test("hint: empty when table has identity columns") {
    withTempDir { dir =>
      val snapshot = DeltaLog.forTable(spark, createIdentityTable(dir)).update()
      val hint = RowLevelConcurrency.enablementHintIfMissing(
        snapshot, spark.sessionState.conf)
      assert(hint == "", s"Identity-column tables must not receive an RLC enablement hint: $hint")
    }
  }
}
