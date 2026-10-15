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

import org.apache.spark.sql.delta.DeleteBaseMixin
import org.apache.spark.sql.delta.rowid.RowTrackingDeleteTestDimension
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf

/**
 * DELETE-specific setup for running the DELETE test suites against AMT tables with the
 * [[AMTDMLTestUtils]] harness.
 *
 * The DELETE AMT mixins must come after all other mixins of a suite, so their dimensions must be
 * the last of every generator config they are used in. This is for 2 reasons:
 * 1. Visibility: some mixins declare beforeAll public, and a later mixin overriding it as
 *    protected (which AMTDMLTestUtils does) fails to compile.
 * 2. Commit orders: some mixins perform extra commits (e.g. DeleteTableWithDVsMixin). To maintain
 *    a stable commit order, we prefer to have AMT commits wrap them all.
 */
trait DeleteAMTTestBase extends AMTDMLTestUtils {

  // Override as public to pass compilation
  override def beforeAll(): Unit = super.beforeAll()
}

/**
 * Generates AMT variants of the DELETE test suites that run DELETE through
 * [[DeleteBaseMixin.executeDelete]].
 *
 * Each DELETE is bracketed by the [[AMTDMLTestUtils]] checkpoints.
 */
trait DeleteAMTMixin extends DeleteBaseMixin with DeleteAMTTestBase {

  // The base DELETE suites assert rewrite semantics (copied rows, commit tags, file counts), so
  // keep DELETE from writing deletion vectors on the DV-enabled AMT tables. Unlike
  // AMTDMLTestUtils, do this whatever the suite's default, since not every build of the base
  // suites sets one. Suites that mix in DeleteDvMixin turn them back on at runtime.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key, "false")

  override def excluded: Seq[String] = super.excluded ++ Seq(
    // These tests fail when row tracking or deletion vectors are enabled, and AMT tables always
    // have both. The baseline DELETE suites exclude them for the same reason, so keep this list in
    // sync with DeleteWithRowTrackingOverrides (row tracking) and DeleteDvOverrides (deletion
    // vectors).
    "DELETE preserves insertion time and zorder tags with BHJ with ephemeral deletion vectors",
    "DELETE preserves insertion time and zorder tags without BHJ with ephemeral deletion vectors",
    "DELETE preserves insertion time tags with 2 files per task",
    "DELETE preserved tags with 2 files per task metric true",
    "DELETE does not preserve insertion time tags without flag",
    "DELETE does not preserve insertion time and zorder tags with BHJ",
    "DELETE does not preserve insertion time and zorder tags without BHJ",
    "Probe table in a broadcast join should not preserve order without AQE",
    "SortMergeJoin does not preserve order without AQE",
    "partition pruning",
    "Deletion vectors are cleaned up with subquery.",
    "delete logs error if number of records are missing in stats"
  )

  abstract override protected def executeDelete(target: String, where: String): Unit = {
    withAMTCheckpointsAround(target) {
      super.executeDelete(target, where)
    }
  }
}

/**
 * Generates AMT variants of the row tracking DELETE suites, which run DELETE through
 * [[RowTrackingDeleteTestDimension.executeDelete]] rather than [[DeleteBaseMixin]].
 *
 * The [[AMTDMLTestUtils]] checkpoints bracket each test table's body rather than each DELETE: the
 * row tracking checks around each DELETE inspect the latest commit, which must be the DELETE
 * itself.
 */
trait RowTrackingDeleteAMTMixin extends RowTrackingDeleteTestDimension with DeleteAMTTestBase {

  override def excluded: Seq[String] = super.excluded ++ Seq(
    // These tests create the table with delta.enableRowTracking = false (one to assert row tracking
    // stays off, the others to enable it later via backfill). AMT mandates row tracking, so table
    // creation fails with DELTA_ADAPTIVE_METADATA_REQUIRES_DEPENDENT_FEATURE_ENABLED.
    "Row tracking marked as not preserved when row tracking disabled",
    "DELETE preserves Row ID on tables with row IDs enabled using backfill,isPartitioned=false",
    "DELETE preserves Row ID on tables with row IDs enabled using backfill,isPartitioned=true"
  )

  override def withRowIdTestTable(isPartitioned: Boolean)(f: => Unit): Unit = {
    super.withRowIdTestTable(isPartitioned) {
      withAMTCheckpointsAround(testTableName)(f)
    }
  }
}
