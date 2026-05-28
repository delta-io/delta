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

package org.apache.spark.sql.delta.v3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter, LogicalPlan, Project}
import org.apache.spark.sql.connector.read.file.FileScan
import org.apache.spark.sql.delta.PreprocessTableWithDVs
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.{FileSourceStrategy, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * Planner strategy that lowers a `DataSourceV2ScanRelation(scan: FileScan)` into a
 * `LogicalRelation(HadoopFsRelation)` subtree, runs Delta's deletion-vector preprocessing on
 * it, and then delegates to Spark's `FileSourceStrategy` to emit the final `FileSourceScanExec`.
 *
 * Registered before [[org.apache.spark.sql.delta.PreprocessTableWithDVsStrategy]] so it
 * consumes V2 file scans first; the existing DV strategy continues to consume any plain
 * `LogicalRelation(HadoopFsRelation)` (NONE-mode plans and internal tools).
 */
case class DeltaFileScanStrategy(session: SparkSession)
    extends SparkStrategy
    with PreprocessTableWithDVs {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ScanOperation(projects, _, filters, scanRel: DataSourceV2ScanRelation)
        if scanRel.scan.isInstanceOf[FileScan] =>
      val fileScan = scanRel.scan.asInstanceOf[FileScan]
      val lr = buildLogicalRelation(scanRel, fileScan)
      val withFilters = combineFilters(filters).map(LogicalFilter(_, lr)).getOrElse(lr)
      val withProject = wrapProject(projects, withFilters)
      val updated = preprocessTablesWithDVs(withProject)
      FileSourceStrategy(updated)
    case _ => Nil
  }

  /**
   * Synthesize a `LogicalRelation` over a `HadoopFsRelation` derived from the [[FileScan]]'s
   * [[org.apache.spark.sql.connector.read.file.FileSet]]. Reuses the V2 scan's output
   * AttributeReferences so any expressions above this scan remain valid.
   */
  private def buildLogicalRelation(
      scanRel: DataSourceV2ScanRelation, fileScan: FileScan): LogicalRelation = {
    val fileSet = fileScan.fileSet
    val relation = HadoopFsRelation(
      location = fileSet.fileIndex,
      partitionSchema = fileSet.partitionSchema,
      dataSchema = fileSet.dataSchema,
      bucketSpec = fileSet.bucketSpec,
      fileFormat = fileSet.fileFormat,
      options = fileSet.options)(session)
    LogicalRelation(
      relation = relation,
      output = scanRel.output,
      catalogTable = fileSet.catalogTable,
      isStreaming = false,
      stream = None)
  }

  private def combineFilters(filters: Seq[Expression]): Option[Expression] = {
    if (filters.isEmpty) None else Some(filters.reduceLeft(And))
  }

  private def wrapProject(projects: Seq[NamedExpression], child: LogicalPlan): LogicalPlan = {
    if (projects.isEmpty || projects == child.output) child else Project(projects, child)
  }
}
