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

package org.apache.spark.sql.connect.delta

import io.delta.connect.proto
import io.delta.tables.DeltaTable

import org.apache.spark.sql.SparkSession

/**
 * Base trait for the planner plugins of Delta Connect.
 */
trait DeltaPlannerBase {
  protected def transformDeltaTable(
      spark: SparkSession, deltaTable: proto.DeltaTable): DeltaTable = {
    deltaTable.getAccessTypeCase match {
      case proto.DeltaTable.AccessTypeCase.PATH =>
        DeltaTable.forPath(spark, deltaTable.getPath.getPath, deltaTable.getPath.getHadoopConfMap)
      case proto.DeltaTable.AccessTypeCase.TABLE_OR_VIEW_NAME =>
        DeltaTable.forName(spark, deltaTable.getTableOrViewName)
    }
  }
}
