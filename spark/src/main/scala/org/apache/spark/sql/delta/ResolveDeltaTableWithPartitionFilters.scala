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

import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Filter}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex

/**
 * Pull out the partition filter that may be part of the FileIndex. This can happen when someone
 * queries a Delta table such as spark.read.format("delta").load("/some/table/partition=2")
 */
object ResolveDeltaTableWithPartitionFilters {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    case relation @ DeltaTable(index: TahoeLogFileIndex) if index.partitionFilters.nonEmpty =>
      val result = Filter(
        index.partitionFilters.reduce(And),
        DeltaTableUtils.replaceFileIndex(relation, index.copy(partitionFilters = Nil))
      )
      Some(result)
    case _ => None
  }
}
