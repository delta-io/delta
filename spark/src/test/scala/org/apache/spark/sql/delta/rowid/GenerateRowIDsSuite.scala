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

package org.apache.spark.sql.delta.rowid

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.RowId

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Coalesce, EqualTo, Expression, FileSourceMetadataAttribute, GetStructField, MetadataAttributeWithLogicalName}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StructType

/**
 * This test suite checks the optimized logical plans produced after applying the [[GenerateRowIDs]]
 * rule. It ensures that the rule is correctly applied to all Delta scans in different scenarios and
 * that the optimizer is able to remove redundant expressions or nodes when possible.
 */
class GenerateRowIDsSuite extends QueryTest with RowIdTestUtils {
  protected val testTable: String = "generateRowIDsTestTable"

  override def beforeAll(): Unit = {
    super.beforeAll()
    withRowTrackingEnabled(enabled = true) {
      spark.range(start = 0, end = 20)
        .toDF("id")
        .write
        .format("delta")
        .saveAsTable(testTable)
    }
  }

  override def afterAll(): Unit = {
    sql(s"DROP TABLE IF EXISTS $testTable")
    super.afterAll()
  }

  /**
   * Test runner checking that the optimized plan for the given dataframe matches the expected plan.
   * The expected plan is defined as a partial function `check`, e.g.:
   * check = {
   *   case Project(_, LogicalRelation) => // Do additional checks
   * }
   *
   * Note: Pass `df` by name to avoid evaluating anything before test setup.
   */
  protected def testRowIdPlan(
      testName: String, df: => DataFrame, rowTrackingEnabled: Boolean = true)(
      check: PartialFunction[LogicalPlan, Unit]): Unit = {
    test(testName) {
      withRowTrackingEnabled(enabled = rowTrackingEnabled) {
        check.applyOrElse(df.queryExecution.optimizedPlan, { plan: LogicalPlan =>
          fail(s"Unexpected optimized plan: $plan")
        })
      }
    }
  }

  /**
   * Checks that the given expression corresponds to the expression used to generate Row IDs:
   *   coalesce(_metadata.row_id, _metadata.base_row_id + _metadata.row_index).
   */
  protected def checkRowIdExpr(expr: Expression): Unit = {
    expr match {
      case Coalesce(
            Seq(
              GetStructField(FileSourceMetadataAttribute(_), _, _),
              Add(
                GetStructField(FileSourceMetadataAttribute(_), _, _),
                GetStructField(FileSourceMetadataAttribute(_), _, _),
                _))) => ()
      case Alias(aliasedExpr, RowId.ROW_ID) => checkRowIdExpr(aliasedExpr)
      case _ => fail(s"Expression didn't match expected Row ID expression: $expr")
    }
  }

  /**
   * Checks that a metadata column is present in `output` and that it contains the given fields and
   * only these.
   */
  protected def checkMetadataFieldsPresent(
      output: Seq[AttributeReference],
      expectedFieldNames: Seq[String])
    : Unit = {
    val metadataSchema = output.collect {
      case FileSourceMetadataAttribute(
        MetadataAttributeWithLogicalName(
          AttributeReference(_, schema: StructType, _, _), _)) => schema
    }
    assert(metadataSchema.nonEmpty, s"No metadata column present in output: $output")
    assert(metadataSchema.head.fieldNames === expectedFieldNames,
      "Unexpected metadata fields present in the metadata output.")
  }

  for (rowTrackingEnabled <- BOOLEAN_DOMAIN)
  testRowIdPlan(s"Regular column selected, rowTrackingEnabled: $rowTrackingEnabled",
      sql(s"SELECT id FROM $testTable"), rowTrackingEnabled) {
    // No projection is added when no metadata column is selected.
    case lr: LogicalRelation =>
      assert(lr.output.map(_.name) === Seq("id"), "Scan list didn't match")
  }

  for (rowTrackingEnabled <- BOOLEAN_DOMAIN)
  testRowIdPlan(s"Metadata column selected, rowTrackingEnabled: $rowTrackingEnabled",
      sql(s"SELECT _metadata.file_path FROM $testTable"), rowTrackingEnabled) {
    // Selecting a metadata column adds a projection to unpack metadata fields (unrelated to Row
    // IDs). Row IDs don't introduce an extra projection.
    case Project(projectList, lr: LogicalRelation) =>
      assert(projectList.map(_.name) === Seq("file_path"), "Project list didn't match")
      assert(lr.output.map(_.name) === Seq("id", "_metadata"), "Scan list didn't match")
      checkMetadataFieldsPresent(lr.output, Seq("file_path"))
  }

  testRowIdPlan("Row ID column selected", sql(s"SELECT _metadata.row_id FROM $testTable")) {
    // Selecting Row IDs injects an expression to generate default Row IDs.
    case Project(Seq(rowIdExpr), lr: LogicalRelation) =>
      assert(rowIdExpr.name == RowId.ROW_ID)
      checkRowIdExpr(rowIdExpr)
      assert(lr.output.map(_.name) === Seq("id", "_metadata"))
      checkMetadataFieldsPresent(lr.output, Seq("row_index", "row_id", "base_row_id"))
  }

  testRowIdPlan("Filter on Row ID column",
      sql(s"SELECT * FROM $testTable WHERE _metadata.row_id = 5")) {
    // Filtering on Row IDs injects an expression to generate default Row IDs in the filter.
    case Project(projectList, Filter(EqualTo(rowIdExpr, _), lr: LogicalRelation)) =>
      assert(projectList.map(_.name) === Seq("id"), "Project list didn't match")
      checkRowIdExpr(rowIdExpr)
      assert(lr.output.map(_.name) === Seq("id", "_metadata"), "Scan list didn't match")
      checkMetadataFieldsPresent(lr.output, Seq("row_index", "row_id", "base_row_id"))
  }

  testRowIdPlan("Filter on Row ID in subquery",
      sql(s"SELECT * FROM $testTable WHERE _metadata.row_id IN (SELECT id FROM $testTable)")) {
    // Filtering on Row IDs using a subquery injects an expression to generate default Row IDs in
    // the subquery.
    case Project(
        projectList,
        Join(right: LogicalRelation, left: LogicalPlan, _, joinCond, _)) =>
      assert(projectList.map(_.name) === Seq("id"), "Project list didn't match")
      assert(right.output.map(_.name) === Seq("id", "_metadata"), "Outer scan output didn't match")
      checkMetadataFieldsPresent(right.output, Seq("row_index", "row_id", "base_row_id"))
      assert(left.output.map(_.name) === Seq("id"), "Subquery scan output didn't match")
      joinCond match {
        case Some(EqualTo(rowIdExpr, _)) =>
          checkRowIdExpr(rowIdExpr)
        case _ => fail(s"Subquery was transformed into a join with an unexpected condition.")
      }
  }

  testRowIdPlan("Rename metadata column",
      sql(s"SELECT renamed_metadata FROM (SELECT _metadata AS renamed_metadata FROM $testTable)"
    )) {
    case Project(projectList, lr: LogicalRelation) =>
      assert(projectList.map(_.name) === Seq("renamed_metadata"), "Project list didn't match")
      assert(lr.output.map(_.name) === Seq("id", "_metadata"))
  }
}
