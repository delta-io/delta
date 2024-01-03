/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.cdc.MergeCDCTests
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait MergeIntoDVsTests extends MergeIntoSQLSuite with DeletionVectorsTestUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey, "true")
    spark.conf.set(DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS.key, "true")
  }

  override def excluded: Seq[String] = {
    val miscFailures = Seq(
      "basic case - merge to view on a Delta table by path, " +
        "partitioned: true skippingEnabled: false useSqlView: true",
      "basic case - merge to view on a Delta table by path, " +
        "partitioned: true skippingEnabled: false useSqlView: false",
      "basic case - merge to view on a Delta table by path, " +
        "partitioned: false skippingEnabled: false useSqlView: true",
      "basic case - merge to view on a Delta table by path, " +
        "partitioned: false skippingEnabled: false useSqlView: false",
      "basic case - merge to Delta table by name, isPartitioned: false skippingEnabled: false",
      "basic case - merge to Delta table by name, isPartitioned: true skippingEnabled: false",
      "not matched by source - all 3 clauses - no changes - " +
        "isPartitioned: true - cdcEnabled: true",
      "not matched by source - all 3 clauses - no changes - " +
        "isPartitioned: false - cdcEnabled: true",
      "test merge on temp view - view with too many internal aliases - Dataset TempView"
    )

    super.excluded ++ miscFailures
  }

  protected override lazy val expectedOpTypes: Set[String] = Set(
    "delta.dml.merge.findTouchedFiles",
    "delta.dml.merge.writeModifiedRowsOnly",
    "delta.dml.merge.writeDeletionVectors",
    "delta.dml.merge")
}

class MergeIntoDVsSuite extends MergeIntoDVsTests

class MergeIntoDVsCDCSuite extends MergeIntoDVsTests with MergeCDCTests {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey, "true")
    spark.conf.set(DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS.key, "true")
  }

  /**
   * Merge commands that result to no actions do not generate a new commit when DVs are enabled.
   * We correct affected tests by changing the expected CDC result (Create table CDC).
   */
  override protected def testMergeCdcUnlimitedClauses(name: String)(
      target: => DataFrame,
      source: => DataFrame,
      mergeCondition: String = "s.key = t.key",
      clauses: Seq[MergeClause],
      expectedTableData: => DataFrame = null,
      expectedCdcDataWithoutVersion: => DataFrame = null,
      expectErrorContains: String = null,
      confs: Seq[(String, String)] = Seq(),
      targetTableSchema: Option[StructType] = None): Unit = {
    import testImplicits._

    // Make sure session is initialized.
    initializeSession()

    val createTableCDC =
      Seq((1, "a", "insert"), (2, "b", "insert")).toDF("key", "targetVal", "_change_type")
    // Test name -> CDC expected value override.
    val cdcOverrides = Map("all conditions failed for all rows" -> createTableCDC)

    super.testMergeCdcUnlimitedClauses(
      name)(
      target,
      source,
      mergeCondition,
      clauses,
      expectedTableData,
      cdcOverrides.getOrElse(name, expectedCdcDataWithoutVersion),
      expectErrorContains,
      confs,
      targetTableSchema)
  }
}
