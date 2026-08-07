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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.Filter

/**
 * Bridges Catalyst filter [[Expression]]s to data source [[Filter]]s for the Delta v2 connector.
 *
 * Lives in the `org.apache.spark.sql.execution.datasources.v2` package so it can reach
 * `DataSourceStrategy.translateFilter`, which is `protected[sql]` and therefore not callable from
 * the connector's `io.delta.spark.internal.v2.read` package (nor from Java).
 *
 * TODO: Revisit both call sites once the scan implements the file-source Scan interface. Use (a)
 * below exists only to serve `DeltaV2Scan.toBatch`, which becomes unreachable at that point (see
 * the TODO there), so that use -- and with it this object's reason to live in a Spark-internal
 * package -- should go away. Use (b), translating filters for the Kernel predicate in
 * `DeltaV2ScanBuilder`, is independent of `Batch` and needs its own assessment.
 */
object DeltaV2FilterTranslator {

  /**
   * Translates each Catalyst filter to a data source [[Filter]], dropping any that cannot be
   * converted. The v2 connector selects files at scan-build time via V1 data skipping, so these
   * translated filters are used (a) as `DeltaV2Batch`'s data filters, which distinguish two batches
   * that select different files under otherwise-equal state and prevent `BatchScanExec` from
   * reusing an exchange/subquery across scans with different pushed filters, and (b) for Parquet
   * predicate pushdown (partition-column filters are harmlessly ignored by the Parquet reader,
   * which only pushes filters referencing physical data columns).
   *
   * @param filters Catalyst filter expressions pushed to the scan.
   * @return the subset that translates to a data source [[Filter]].
   */
  def translate(filters: Array[Expression]): Array[Filter] =
    filters
      .flatMap(f => DataSourceStrategy.translateFilter(f, supportNestedPredicatePushdown = true))
}
