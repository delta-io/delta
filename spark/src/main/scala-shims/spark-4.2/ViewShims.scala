/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Shim for View to handle API changes between Spark versions.
 * In Spark 4.2, View has an additional constructor parameter (same as Spark 4.1).
 */
object ViewShims {

  /**
   * Extractor that matches View(desc, true, child) pattern.
   * Used in DeltaViewHelper for matching temp views with a specific structure.
   * In Spark 4.2, View has an additional parameter (same as 4.1), so we use a wildcard to ignore
   * it.
   */
  object TempViewWithChild {
    def unapply(plan: LogicalPlan): Option[(CatalogTable, LogicalPlan)] = plan match {
      // In Spark 4.2, View has an additional parameter (same as 4.1), we use _ to match it
      case View(desc, isTempView, child, _) if isTempView => Some((desc, child))
      case _ => None
    }
  }
}
