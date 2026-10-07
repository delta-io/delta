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

import scala.util.Try

import org.apache.spark.sql.delta.{DeltaConfigs, UpdateBaseMixin}
import org.apache.spark.sql.delta.coordinatedcommits.{CatalogOwnedCommitCoordinatorProvider, CatalogOwnedTableUtils, InMemoryCommitCoordinator, TrackingCommitCoordinatorClient}
import org.apache.spark.sql.delta.rowid.RowTrackingUpdateSuiteBase
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf

/**
 * UPDATE-specific setup for running the UPDATE test suites against AMT tables with the
 * [[AMTDMLTestUtils]] harness.
 *
 * The UPDATE AMT mixins must come after all other mixins of a suite, so their dimensions must be
 * the last of every generator config they are used in. This is for 2 reasons:
 * 1. Visibility: some mixins declare beforeAll public, and a later mixin overriding it as
 *    protected (which AMTDMLTestUtils does) fails to compile.
 * 2. Commit orders: some mixins perform extra commits (e.g. UpdateTableWithDVsMixin). To maintain
 *    a stable commit order, we prefer to have AMT commits wrap them all.
 */
trait UpdateAMTTestBase extends AMTDMLTestUtils {

  override protected def sparkConf: SparkConf = super.sparkConf
    // The UPDATE suites default new tables to `delta.enableDeletionVectors = false`, which AMT
    // rejects at CREATE since it requires deletion vectors. Unset it instead of setting it to true:
    // AMT then enables deletion vectors on the table itself, while the tests, which read this
    // default to predict whether UPDATE writes deletion vectors, keep seeing the command's
    // behavior. Whether UPDATE writes them is a command conf that the mixins below decide.
    .remove(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey)

  // Override as public to pass compilation
  override def beforeAll(): Unit = super.beforeAll()
}

/**
 * Generates AMT variants of the UPDATE test suites that run UPDATE through
 * [[UpdateBaseMixin.executeUpdate]].
 *
 * Each UPDATE is bracketed by the [[AMTDMLTestUtils]] checkpoints.
 */
trait UpdateAMTMixin extends UpdateBaseMixin with UpdateAMTTestBase {

  // The base UPDATE suites assert rewrite semantics (copied rows, commit tags, file counts), so
  // keep UPDATE from writing deletion vectors on the DV-enabled AMT tables. Suites that mix in
  // DeletionVectorOnTestMixin turn them back on at runtime.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS.key, "false")

  // These tests fail when row tracking or deletion vectors are enabled, and AMT tables always
  // have both. The baseline UPDATE suites exclude them for the same reason, so keep this list in
  // sync with UpdateWithRowTrackingOverrides (row tracking) and UpdateDvOverrides and
  // UpdateSQLWithDeletionVectorsMixin (deletion vectors).
  override def excluded: Seq[String] = super.excluded ++ Seq(
    "UPDATE preserves insertion time tags with 2 files per task",
    "UPDATE does not preserve insertion time tags with 2 file per task without flag",
    "UPDATE preserves tags metric true",
    "partition pruning",
    "schema pruning on finding files to update",
    "nested schema pruning on finding files to update",
    "Deletion vectors are cleaned up with subquery",
    "update logs error if number of records are missing in stats",
    // These are exclueded due to same reason as in UpdateWithRowTrackingOverrides.
    "test update on temp view - view with too many internal aliases - Dataset TempView",
    "test update on temp view - view with too many internal aliases - SQL TempView",
    "test update on temp view - view with too many internal aliases " +
      "with write amplification reduction - Dataset TempView",
    "test update on temp view - view with too many internal aliases " +
      "with write amplification reduction - SQL TempView",
    "test update on temp view - basic - Partition=true - SQL TempView",
    "test update on temp view - basic - Partition=false - SQL TempView",
    "test update on temp view - superset cols - Dataset TempView",
    "test update on temp view - superset cols - SQL TempView",
    "test update on temp view - nontrivial projection - Dataset TempView",
    "test update on temp view - nontrivial projection - SQL TempView",
    "test update on temp view - nontrivial projection " +
      "with write amplification reduction - Dataset TempView",
    "test update on temp view - nontrivial projection " +
      "with write amplification reduction - SQL TempView",
    "update a SQL temp view"
  )

  // Dropping a catalog-managed table leaves it in the in-memory commit coordinator, which keys
  // tables by log path. A table recreated under the same name in the same test would then see the
  // dropped table's commits, so remove the dropped table from the coordinator as well.
  abstract override protected def dropTable(): Unit = {
    val logPathOpt = Try(deltaLog.logPath).toOption
    super.dropTable()
    val catalogName = CatalogOwnedTableUtils.DEFAULT_CATALOG_NAME_FOR_TESTING
    val coordinatorOpt = CatalogOwnedCommitCoordinatorProvider.getBuilder(catalogName)
      .map(_.buildForCatalog(spark, catalogName))
      .map {
        case tracking: TrackingCommitCoordinatorClient => tracking.delegatingCommitCoordinatorClient
        case other => other
      }
      .collect { case inMemory: InMemoryCommitCoordinator => inMemory }
    for (logPath <- logPathOpt; coordinator <- coordinatorOpt) {
      coordinator.dropTable(logPath)
    }
  }

  abstract override protected def executeUpdate(
      target: String,
      set: String,
      where: String): Unit = {
    withAMTCheckpointsAround(target) {
      super.executeUpdate(target, set, where)
    }
  }
}

/**
 * Generates AMT variants of the row tracking UPDATE suites, which run UPDATE through
 * [[RowTrackingUpdateSuiteBase.executeUpdate]] rather than [[UpdateBaseMixin]].
 *
 * The [[AMTDMLTestUtils]] checkpoints bracket each test table's body rather than each UPDATE: the
 * row tracking checks around each UPDATE inspect the latest commit, which must be the UPDATE
 * itself.
 */
trait RowTrackingUpdateAMTMixin extends RowTrackingUpdateSuiteBase with UpdateAMTTestBase {

  override def excluded: Seq[String] = super.excluded ++ Seq(
    // These tests create the table with delta.enableRowTracking = false (one to assert row tracking
    // stays off, the others to enable it later via backfill). AMT mandates row tracking, so table
    // creation fails with DELTA_ADAPTIVE_METADATA_REQUIRES_DEPENDENT_FEATURE_ENABLED.
    "Row tracking marked as not preserved when row tracking disabled",
    "UPDATE preserves Row Tracking on tables enabled using backfill, isPartitioned=false",
    "UPDATE preserves Row Tracking on tables enabled using backfill, isPartitioned=true"
  )

  // The tests set `last_modified_version` to the version they expect the UPDATE to commit at and
  // check that it matches the rows' row commit versions. The full checkpoint shifts the UPDATE's
  // commit version, so pass the version it actually commits at instead.
  override protected def executeUpdate(
      tableName: String,
      where: Option[String],
      newVersion: Long): Unit = {
    val commitVersion =
      resolveAMTTarget(tableName).map(_.snapshot.version + 1).getOrElse(newVersion)
    super.executeUpdate(tableName, where, commitVersion)
  }

  override protected def withRowIdTestTable(isPartitioned: Boolean)(f: => Unit): Unit = {
    super.withRowIdTestTable(isPartitioned) {
      withAMTCheckpointsAround(targetTableName)(f)
    }
  }
}
