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

package org.apache.spark.sql.delta.catalog

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2RelationShim

/**
 * Trait allowing V1 code path to identify a V2 [[DeltaV2Table]] without taking a dependency on
 * that class that lives in a different target.
 */
trait DeltaV2TableMarker

object DeltaV2TableMarker {

  /** Whether the given plan is a relation over a [[DeltaV2Table]]. */
  def isDeltaV2TableRelation(child: LogicalPlan): Boolean = child match {
    case DataSourceV2RelationShim(_: DeltaV2TableMarker, _, _, _, _) => true
    case _ => false
  }
}
