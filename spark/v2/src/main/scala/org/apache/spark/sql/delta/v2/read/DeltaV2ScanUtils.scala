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
package org.apache.spark.sql.delta.v2.read

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager
import io.delta.kernel.engine.{Engine => KernelEngine}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, ScanBuilder, Statistics}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Public factory methods for Delta's package-private Spark DataSource V2 scan internals. */
object DeltaV2ScanUtils {

  def newReaderFactory(
      readFunc: PartitionedFile => Iterator[InternalRow],
      supportsColumnar: Boolean): PartitionReaderFactory = {
    new DeltaV2ReaderFactory(readFunc, supportsColumnar)
  }

  def newScanBuilder(
      tableName: String,
      initialSnapshot: Snapshot,
      kernelEngine: KernelEngine,
      catalogTable: Option[CatalogTable],
      snapshotManager: DeltaV2SnapshotManager,
      dataSchema: StructType,
      partitionSchema: StructType,
      tableSchema: StructType,
      catalogStats: Option[Statistics],
      options: CaseInsensitiveStringMap): ScanBuilder = {
    DeltaV2ScanBuilder.create(
      tableName = tableName,
      initialSnapshot = initialSnapshot,
      kernelEngine = kernelEngine,
      catalogTable = catalogTable,
      snapshotManager = snapshotManager,
      dataSchema = dataSchema,
      partitionSchema = partitionSchema,
      tableSchema = tableSchema,
      catalogStats = catalogStats,
      options = options
    )
  }
}
