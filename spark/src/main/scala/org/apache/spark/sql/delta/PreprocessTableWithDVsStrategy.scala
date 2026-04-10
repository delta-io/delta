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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.{FileSourceStrategy, HadoopFsRelation, LogicalRelationWithTable}

/**
 * Strategy to process tables with DVs and add the skip row column and filters.
 *
 * This strategy will apply all transformations needed to tables with DVs and delegate to
 * [[FileSourceStrategy]] to create the final plan. The DV filter will be the bottom-most filter in
 * the plan and so it will be pushed down to the FileSourceScanExec at the beginning of the filter
 * list.
 */
case class PreprocessTableWithDVsStrategy(session: SparkSession)
    extends SparkStrategy
    with PreprocessTableWithDVs {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ScanOperation(_, _, _, _ @ LogicalRelationWithTable(_: HadoopFsRelation, _)) =>
      val updatedPlan = preprocessTablesWithDVs(plan)
      FileSourceStrategy(updatedPlan)
    case _ => Nil
  }
}
