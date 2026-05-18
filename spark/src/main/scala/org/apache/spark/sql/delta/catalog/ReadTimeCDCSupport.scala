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

package org.apache.spark.sql.delta.catalog

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Routing SPI for read-time CDF. Mixed into a V2 catalog implementation (e.g. the spark-unified
 * `ChangelogSupport`) to let V1 CDC entry points (`table_changes(...)`, `DeltaDataSource.getTable`
 * for `.option("readChangeFeed", "true")`) hand a CDC read off to the kernel-based DSv2 changelog
 * reader without depending on V2 classes from V1.
 *
 * Lives in sparkV1 so the entry points can pattern-match on the trait directly. The actual
 * implementation that wraps a V2 [[io.delta.spark.internal.v2.read.changelog.DeltaChangelog]] in
 * a [[org.apache.spark.sql.execution.datasources.v2.ChangelogTable]] lives in spark-unified.
 */
trait ReadTimeCDCSupport {

  /**
   * If the given (DeltaTableV2, options) describes a CDC read that should be served by the
   * read-time CDC reader, return a V2 Table backed by the DSv2 changelog stack. Returns None
   * when read-time CDF routing does not apply -- e.g. the request isn't a CDC read, CDF is
   * enabled on the table, row tracking is missing, or the session flag is off. Callers fall
   * back to the legacy write-time path (which surfaces `CHANGE_DATA_NOT_RECORDED` for tables
   * without CDF) on None.
   */
  def buildReadTimeCDCTable(
      deltaTable: DeltaTableV2,
      options: CaseInsensitiveStringMap): Option[Table]
}
