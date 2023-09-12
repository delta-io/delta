/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedTable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{CatalogHelper, MultipartIdentifierHelper}
import org.apache.spark.sql.delta.catalog.DeltaTableV2

/**
 * Replaces [[UnresolvedTable]]s if the plan is for direct query on files.
 */
case class ResolveDeltaPathTable(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  private def maybeSQLFile(u: UnresolvedTable): Boolean = {
    sparkSession.sessionState.conf.runSQLonFile && u.multipartIdentifier.size == 2
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u: UnresolvedTable if maybeSQLFile(u) =>
      val tableId = u.multipartIdentifier.asTableIdentifier
      if (DeltaTableUtils.isValidPath(tableId)) {
        val deltaTableV2 = DeltaTableV2(sparkSession, new Path(tableId.table))
        val sessionCatalog =
          sparkSession.sessionState.catalogManager.v2SessionCatalog.asTableCatalog
        ResolvedTable.create(sessionCatalog, u.multipartIdentifier.asIdentifier, deltaTableV2)
      } else {
        u
      }
  }
}
