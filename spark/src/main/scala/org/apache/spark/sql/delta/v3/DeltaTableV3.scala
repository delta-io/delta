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

package org.apache.spark.sql.delta.v3

import java.{util => ju}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.delta.{DeltaLog, DeltaTableUtils, DeltaTimeTravelSpec, Snapshot}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Delta v3 connector. A DSv2 `Table` whose `Scan` is a [[FileScan]] and whose `Write` is a
 * [[FileWrite]] - both lowered to file-source physical plans by the v3 planner strategies.
 *
 * The v3 connector is the new approach from the DSv2 migration hackathon: DSv2 logical plan
 * shape preserved through analysis/optimization (no analysis-time V2->V1 fallback), but
 * V1 file-scan / file-write execution under the hood for now. Photon can swap the leaves
 * later by replacing `FileSourceScanExec` with its own variants.
 *
 * Reuses Delta core internals: `DeltaLog`, `Snapshot`, `OptimisticTransaction`, etc. There is
 * no dependency on Delta Kernel; that path stays in the [[DeltaV2Table]] (sparkV2) connector.
 *
 * NOT a [[org.apache.spark.sql.connector.catalog.V2TableWithV1Fallback]]: the whole point of
 * v3 is to keep the V2 plan shape. The existing [[org.apache.spark.sql.delta.catalog.DeltaTableV2]]
 * (the v1 connector) is unchanged and continues to drive all today's traffic; v3 is only
 * returned by the catalog when explicitly enabled.
 */
class DeltaTableV3 private(
    val spark: SparkSession,
    val path: Path,
    val catalogTable: Option[CatalogTable],
    val timeTravelOpt: Option[DeltaTimeTravelSpec],
    val options: Map[String, String])
  extends Table
  with SupportsRead
  with SupportsWrite {

  lazy val deltaLog: DeltaLog = DeltaLog.forTable(spark, path, options, catalogTable)

  /** Pinned snapshot for this table - the v3 contract is that the version is determinable
   *  from the table instance without running the optimizer. */
  lazy val initialSnapshot: Snapshot = {
    timeTravelOpt.map { spec =>
      val (version, _) =
        DeltaTableUtils.resolveTimeTravelVersion(
          spark.sessionState.conf, deltaLog, catalogTable, spec)
      deltaLog.getSnapshotAt(
        version,
        catalogTableOpt = catalogTable,
        enforceTimeTravelWithinDeletedFileRetention = spec.enforceRetention)
    }.getOrElse(
      deltaLog.update(catalogTableOpt = catalogTable)
    )
  }

  /** True when this table is pinned to a specific version (time travel). */
  def isTimeTraveled: Boolean = timeTravelOpt.isDefined

  override def name(): String = catalogTable.map(_.identifier.unquotedString)
    .getOrElse(s"delta.`${deltaLog.dataPath}`")

  private lazy val tableSchema: StructType =
    DeltaTableUtils.removeInternalDeltaMetadata(
      spark,
      DeltaTableUtils.removeInternalWriterMetadata(spark, initialSnapshot.schema))

  override def schema(): StructType = tableSchema

  override def partitioning(): Array[Transform] = {
    initialSnapshot.metadata.partitionColumns.map { col =>
      new IdentityTransform(new FieldReference(Seq(col)))
    }.toArray
  }

  override def properties(): ju.Map[String, String] = {
    initialSnapshot.getProperties.asJava
  }

  override def capabilities(): ju.Set[TableCapability] = Set[TableCapability](
    ACCEPT_ANY_SCHEMA, BATCH_READ,
    BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE, OVERWRITE_DYNAMIC
  ).asJava

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder =
    new DeltaScanBuilderV3(this, scanOptions)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val nullAsDefault = spark.sessionState.conf
      .getConf(DeltaSQLConf.DELTA_DF_WRITE_ALLOW_IMPLICIT_CASTS) // proxy for null-as-default
    new DeltaWriteBuilderV3(this, info, nullAsDefault)
  }
}

object DeltaTableV3 {

  def apply(
      spark: SparkSession,
      path: Path,
      catalogTable: Option[CatalogTable],
      timeTravelOpt: Option[DeltaTimeTravelSpec] = None,
      options: Map[String, String] = Map.empty): DeltaTableV3 = {
    new DeltaTableV3(spark, path, catalogTable, timeTravelOpt, options)
  }
}
