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

package org.apache.spark.sql.delta.files

import org.apache.spark.sql.delta.{DeltaLog, DeltaTableUtils, Snapshot}
import org.apache.spark.sql.delta.actions.{AddCDCFile, RemoveFile}
import org.apache.spark.sql.delta.sources.IndexedFile
import org.apache.spark.sql.delta.util.StateCache

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.functions._

/**
 * Converts a `Snapshot` into the initial set of files read when starting a new streaming query.
 * The list of files that represent the table at the time the query starts are selected by:
 * - Adding `version` and `index` to each file to enable splitting of the initial state into
 *   multiple batches.
 * - Filtering files that don't match partition predicates, while preserving the aforementioned
 *   indexing.
 */
class DeltaSourceSnapshot(
    val spark: SparkSession,
    val snapshot: Snapshot,
    val filters: Seq[Expression])
  extends SnapshotIterator
  with StateCache {

  protected val version = snapshot.version
  protected val path = snapshot.path

  protected lazy val (partitionFilters, dataFilters) = {
    val partitionCols = snapshot.metadata.partitionColumns
    filters.partition { e =>
      DeltaTableUtils.isPredicatePartitionColumnsOnly(e, partitionCols, spark)
    }
  }

  protected def initialFiles: Dataset[IndexedFile] = {
    import spark.implicits._

    cacheDS(
      snapshot.allFiles.sort("modificationTime", "path")
        .rdd.zipWithIndex()
        .toDF("add", "index")
        .withColumn("remove", typedLit(Option.empty[RemoveFile]))
        .withColumn("cdc", typedLit(Option.empty[AddCDCFile]))
        .withColumn("version", lit(version))
        .withColumn("isLast", lit(false))
        .as[IndexedFile],
      s"Delta Source Snapshot #$version - ${snapshot.redactedPath}").getDS
  }

  override def close(unpersistSnapshot: Boolean): Unit = {
    super.close(unpersistSnapshot)

    if (unpersistSnapshot) {
      snapshot.uncache()
    }
  }
}

trait SnapshotIterator {
  self: DeltaSourceSnapshot =>

  private var result: Iterable[IndexedFile] = _

  def iterator(): Iterator[IndexedFile] = {
    import spark.implicits._
    import collection.JavaConverters._
    if (result == null) {
      result = DeltaLog.filterFileList(
        snapshot.metadata.partitionSchema,
        initialFiles.toDF(),
        partitionFilters,
        Seq("add")).as[IndexedFile].toLocalIterator().asScala.toIterable
    }
    // This will always start from the beginning and re-use resources. If any exceptions were to
    // be thrown, the stream would stop, we would call stop on the source, and that will make
    // sure that we clean up resources.
    result.toIterator
  }

  def close(unpersistSnapshot: Boolean): Unit = { }
}
