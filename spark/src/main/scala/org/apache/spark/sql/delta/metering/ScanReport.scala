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

package org.apache.spark.sql.delta.metering

import org.apache.spark.sql.delta.stats.DataSize
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

case class ScanReport(
    tableId: String,
    path: String,
    scanType: String,
    deltaDataSkippingType: String,
    partitionFilters: Seq[String],
    dataFilters: Seq[String],
    unusedFilters: Seq[String],
    size: Map[String, DataSize],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    metrics: Map[String, Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    versionScanned: Option[Long],
    annotations: Map[String, Long],
    usedPartitionColumns: Seq[String],
    numUsedPartitionColumns: Long,
    allPartitionColumns: Seq[String],
    numAllPartitionColumns: Long,
    // Number of output rows from parent filter node if it is available and has the same
    // predicates as dataFilters.
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    parentFilterOutputRows: Option[Long])

object ScanReport {
  // Several of the ScanReport fields are only relevant for certain types of delta scans.
  // Provide an alternative constructor for callers that don't need to set those fields.
  // scalastyle:off argcount
  def apply(
      tableId: String,
      path: String,
      scanType: String,
      partitionFilters: Seq[String],
      dataFilters: Seq[String],
      unusedFilters: Seq[String],
      size: Map[String, DataSize],
      metrics: Map[String, Long],
      versionScanned: Option[Long],
      annotations: Map[String, Long],
      parentFilterOutputRows: Option[Long]
      ): ScanReport = {
    // scalastyle:on
    ScanReport(
      tableId = tableId,
      path = path,
      scanType = scanType,
      deltaDataSkippingType = "",
      partitionFilters = partitionFilters,
      dataFilters = dataFilters,
      unusedFilters = unusedFilters,
      size = size,
      metrics = metrics,
      versionScanned = versionScanned,
      annotations = annotations,
      usedPartitionColumns = Nil,
      numUsedPartitionColumns = 0L,
      allPartitionColumns = Nil,
      numAllPartitionColumns = 0L,
      parentFilterOutputRows = parentFilterOutputRows)
  }

  // Similar as above, but without parentFilterOutputRows
  def apply(
      tableId: String,
      path: String,
      scanType: String,
      partitionFilters: Seq[String],
      dataFilters: Seq[String],
      unusedFilters: Seq[String],
      size: Map[String, DataSize],
      metrics: Map[String, Long],
      versionScanned: Option[Long],
      annotations: Map[String, Long]): ScanReport = {
    ScanReport(
      tableId = tableId,
      path = path,
      scanType = scanType,
      partitionFilters = partitionFilters,
      dataFilters = dataFilters,
      unusedFilters = unusedFilters,
      size = size,
      metrics = metrics,
      versionScanned = versionScanned,
      annotations = annotations,
      parentFilterOutputRows = None)
  }
}
