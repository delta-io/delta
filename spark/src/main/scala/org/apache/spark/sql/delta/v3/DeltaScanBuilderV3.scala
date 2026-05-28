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

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Builds a [[DeltaFileScan]] for a [[DeltaTableV3]]. Pins the [[Snapshot]] at `build()` time
 * so the scanned version is fixed before any optimizer rule runs.
 *
 * Implements [[SupportsPushDownFilters]] and [[SupportsPushDownRequiredColumns]] so Spark's
 * V2 push-down framework can prune columns and surface filter pushdown to the connector. The
 * pushdown is conservative: filters that reference partition columns only are accepted; all
 * other filters are also retained as `postScanFilters` so Spark re-evaluates them above the
 * scan. (We can tighten this later once we mirror `FileSourceStrategy`'s split logic.)
 */
class DeltaScanBuilderV3(
    deltaTable: DeltaTableV3,
    options: CaseInsensitiveStringMap)
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private val spark = deltaTable.spark
  private var requiredSchema: StructType = deltaTable.schema()
  private var _pushedFilters: Array[Filter] = Array.empty
  private var _postScanFilters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val partitionCols = deltaTable.initialSnapshot.metadata.partitionColumns.toSet
    val (partitionFilters, others) = filters.partition { f =>
      f.references.forall(partitionCols.contains)
    }
    _pushedFilters = partitionFilters
    _postScanFilters = others
    // Filters not accepted for pushdown must be returned so Spark re-evaluates above the scan.
    others
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pruneColumns(required: StructType): Unit = {
    requiredSchema = required
  }

  override def build(): Scan = {
    val snapshot = deltaTable.initialSnapshot
    val isCDC = CDCReader.isCDCRead(options)
    val fileSet = buildFileSet(snapshot, isCDC)
    DeltaFileScan(
      fileSet = fileSet,
      pinnedSnapshot = snapshot,
      requiredSchema = requiredSchema,
      pushedFilters = _pushedFilters,
      postScanFilters = _postScanFilters,
      isCDC = isCDC)
  }

  /**
   * Build the [[DeltaFileSet]] corresponding to this scan. For a regular (non-CDC) read we
   * synthesize the same `TahoeLogFileIndex` + `FileFormat` triple that
   * `DeltaLog.createRelation` produces today. For CDC reads we delegate to
   * `CDCReader.getCDCRelation` and reuse its resulting `HadoopFsRelation` parts.
   */
  private def buildFileSet(snapshot: Snapshot, isCDC: Boolean): DeltaFileSet = {
    if (isCDC) {
      val rel: BaseRelation = CDCReader.getCDCRelation(
        spark,
        snapshot,
        deltaTable.catalogTable,
        isTimeTravelQuery = deltaTable.isTimeTraveled,
        spark.sessionState.conf,
        options)
      // CDCReader returns a HadoopFsRelation for the common CDF path. Defensive cast: the
      // strategy can't lower a non-HadoopFsRelation today.
      val hfs = rel match {
        case h: HadoopFsRelation => h
        case other =>
          throw new IllegalStateException(
            s"DeltaScanBuilderV3: expected CDC relation to be HadoopFsRelation, got " +
              s"${other.getClass.getName}")
      }
      DeltaFileSet(
        fileIndex = hfs.location,
        fileFormat = hfs.fileFormat,
        partitionSchema = hfs.partitionSchema,
        dataSchema = hfs.dataSchema,
        bucketSpec = hfs.bucketSpec,
        options = options.asCaseSensitiveMap().asScala.toMap,
        catalogTable = deltaTable.catalogTable,
        isPinned = true)
    } else {
      val fileIndex = TahoeLogFileIndex(
        spark,
        snapshot.deltaLog,
        snapshot.deltaLog.dataPath,
        snapshot,
        catalogTableOpt = deltaTable.catalogTable,
        partitionFilters = Seq.empty,
        isTimeTravelQuery = deltaTable.isTimeTraveled)
      val hfs = snapshot.deltaLog.buildHadoopFsRelationWithFileIndex(
        snapshot, fileIndex, bucketSpec = None)
      DeltaFileSet(
        fileIndex = fileIndex,
        fileFormat = hfs.fileFormat,
        partitionSchema = hfs.partitionSchema,
        dataSchema = hfs.dataSchema,
        bucketSpec = hfs.bucketSpec,
        options = options.asCaseSensitiveMap().asScala.toMap,
        catalogTable = deltaTable.catalogTable,
        isPinned = deltaTable.isTimeTraveled)
    }
  }
}
