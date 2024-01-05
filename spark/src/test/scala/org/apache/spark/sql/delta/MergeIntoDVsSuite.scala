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
package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.cdc.MergeCDCTests
import org.apache.spark.sql.delta.sources.DeltaSQLConf

trait MergeIntoDVsTests extends MergeIntoSQLSuite with DeletionVectorsTestUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectors(spark, merge = true)
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

trait MergeCDCWithDVsTests extends MergeCDCTests with DeletionVectorsTestUtils {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectors(spark, merge = true)
  }

  override def excluded: Seq[String] = {
    /**
     * Merge commands that result to no actions do not generate a new commit when DVs are enabled.
     * We correct affected tests by changing the expected CDC result (Create table CDC).
     */
    val miscFailures = "merge CDC - all conditions failed for all rows"

    super.excluded :+ miscFailures
  }
}

/**
 * Includes the entire MergeIntoSQLSuite with CDC enabled.
 */
class MergeIntoDVsCDCSuite extends MergeIntoDVsTests with MergeCDCWithDVsTests
