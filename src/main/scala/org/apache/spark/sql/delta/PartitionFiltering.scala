/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.actions.{AddFile, SingleAction}
import org.apache.spark.sql.delta.stats.DeltaScan

import org.apache.spark.sql.catalyst.expressions._

/** Provides the capability for partition pruning when querying a Delta table. */
trait PartitionFiltering { self: Snapshot =>

  def filesForScan(projection: Seq[Attribute], filters: Seq[Expression]): DeltaScan = {
    implicit val enc = SingleAction.addFileEncoder

    val partitionFilters = filters.flatMap { filter =>
      DeltaTableUtils.splitMetadataAndDataPredicates(filter, metadata.partitionColumns, spark)._1
    }

    val files = withStatusCode("DELTA", "Filtering files for query") {
      DeltaLog.filterFileList(
        metadata.partitionSchema,
        allFiles.toDF(),
        partitionFilters).as[AddFile].collect()
    }

    DeltaScan(version = version, files, null, null, null)(null, null, null, null)
  }
}
