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
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.connector.write.file.FileWrite
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Planner strategy that lowers V2 write commands (`AppendData`, `OverwriteByExpression`,
 * `OverwritePartitionsDynamic`) over a [[DeltaTableV3]] into the corresponding `*FilesExec`
 * node. Returns `Nil` for any other table (v1 connector via DeltaTableV2 stays on its
 * legacy V1 fallback path).
 */
case class DeltaFileWriteStrategy(session: SparkSession) extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case AppendData(r, q, _, _, w, _) =>
      forFileWrite(r, w) { fw => AppendFilesExec(planLater(q), fw) :: Nil }
    case OverwriteByExpression(r, _, q, _, _, w, _) =>
      forFileWrite(r, w) { fw => OverwriteFilesByExpressionExec(planLater(q), fw) :: Nil }
    case OverwritePartitionsDynamic(r, q, _, _, w) =>
      forFileWrite(r, w) { fw => OverwritePartitionFilesDynamicExec(planLater(q), fw) :: Nil }
    case _ => Nil
  }

  /**
   * Common matcher: the write target is a `DataSourceV2Relation(DeltaTableV3)` and the
   * resolved `Write` is a [[FileWrite]]. Otherwise returns `Nil` so another strategy handles
   * the plan (including the V1 fallback path used by [[DeltaTableV2]]).
   */
  private def forFileWrite(
      target: LogicalPlan, write: Option[Write])(
      f: FileWrite => Seq[SparkPlan]): Seq[SparkPlan] = {
    target match {
      case r: DataSourceV2Relation =>
        r.table match {
          case _: DeltaTableV3 =>
            write match {
              case Some(fw: FileWrite) => f(fw)
              case _ => Nil
            }
          case _ => Nil
        }
      case _ => Nil
    }
  }
}
