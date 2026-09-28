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

package org.apache.spark.sql.delta.stats

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{AdaptiveMetadataTableFeature, DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.{defaultPropertyKey, FEATURE_PROP_SUPPORTED}
import org.apache.spark.sql.delta.amt.{AMTCheckpointProvider, AMTTriggerMode}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType

/**
 * Runs the DataSkipping V1 corpus against AMT tables.
 */
trait DataSkippingAMTBase extends DataSkippingDeltaTestsBase
{

  import testImplicits._

  // AMT requires the catalogManaged feature. `catalogOwnedCoordinatorBackfillBatchSize = Some(1)`
  // registers the in-memory commit coordinator (and enables catalog-owned-by-default) so
  // catalog-managed AMT tables can be created locally, backfilling every commit immediately.
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)

  override protected def sparkConf: SparkConf =
    super.sparkConf
      // Every table created in this suite is an AMT table by default.
      .set(defaultPropertyKey(AdaptiveMetadataTableFeature), FEATURE_PROP_SUPPORTED)
      // Pack few entries per manifest leaf so these small tables produce a real multi-leaf AMT
      // tree (root + leaf pointers) instead of a single promoted manifest, exercising the tree
      // reconstruction path.
      .set(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key, "2")

  protected def amtExcludedTests: Seq[String] = Seq(
    // Column mapping mode conflict: AMT forces `id`, but these create the table in `name`/`none`
    // mode explicitly and throw DELTA_ADAPTIVE_METADATA_REQUIRES_COLUMN_MAPPING_ID_MODE at create.
    "data skipping with missing workload-based column stats and column mapping (name)",
    "data skipping with missing workload-based column stats, column mapping, and RENAME COLUMN",
    "data skipping with missing stats for any column and column mapping (name)",
    "data skipping with column mapping and upper case column names - +columnMapping = none",
    "data skipping with column mapping and upper case column names - +columnMapping = name",
    "Data skipping with delta statistic column drop column",
    "Data skipping with delta statistic column rename column",
    "data skipping flags",
    // Row tracking is always enabled on AMT, but these tests disable it explicitly and hit
    // DELTA_ADAPTIVE_METADATA_REQUIRES_DEPENDENT_FEATURE_ENABLED at create.
    "base_row_id filter throws FIELD_NOT_FOUND when row tracking is disabled",
    "default_row_commit_version filter throws FIELD_NOT_FOUND when row tracking is disabled"
  )

  // Substring match; ignoring the base test name here also prevents the corpus from registering
  // its DataFrame-schema twin.
  override protected def test(testName: String, testTags: org.scalatest.Tag*)
      (testFun: => Any)(implicit pos: org.scalactic.source.Position): Unit = {
    if (amtExcludedTests.exists(testName.contains)) {
      ignore(testName, testTags: _*)(testFun)(pos)
    } else {
      super.test(testName, testTags: _*)(testFun)(pos)
    }
  }

  /**
   * Emits an AMT checkpoint on `log` (a full rewrite -- self-contained, needs no prior checkpoint),
   * then re-resolves the log so the next read picks up the new checkpoint.
   */
  override protected def checkpointAndCreateNewLogIfNecessary(log: DeltaLog): DeltaLog = {
    log.startTransaction().commit(
      Seq.empty,
      DeltaOperations.OptimizeCheckpoint(
        incremental = false, triggerName = AMTTriggerMode.CheckpointIntervalFull.name))
    DeltaLog.clearCache()
    DeltaLog.forTable(spark, log.dataPath)
  }

  /** Asserts the AMT V1 skipping path serves reads: V2 disabled, checkpoint is AMT-backed. */
  protected def assertAMTV1(log: DeltaLog): Unit = {
    val snapshot = log.update()
    assert(snapshot.checkpointProvider.isInstanceOf[AMTCheckpointProvider],
      "Expected an AMTCheckpointProvider after checkpoint, got " +
        snapshot.checkpointProvider.getClass.getSimpleName)
  }

  // AMT manifest stats only exist after a checkpoint, so run each predicate case against a real AMT
  // checkpoint, in two source combinations: all files in the manifest; and the manifest with its
  // files removed and re-added by a delta on top (mixed log replay of file actions).
  override protected def testSkipping(
      name: String,
      data: String,
      schema: StructType = null,
      hits: Seq[String],
      misses: Seq[String],
      sqlConfs: Seq[(String, String)] = Nil,
      indexedCols: Int = defaultNumIndexedCols,
      deltaStatsColNamesOpt: Option[String] = None,
      checkEmptyUnusedFiltersForHits: Boolean = false,
      exceptionOpt: Option[Throwable] = None): Unit = {
    // No corpus case passes an `exceptionOpt` today. The error path asserts a throw during setup
    // via the base's `intercept`, which the AMT checkpoint flow below does not replicate -- so if a
    // case ever starts using it, fail loudly here rather than silently mis-running it as a success.
    require(exceptionOpt.isEmpty,
      s"exceptionOpt is not yet supported by the AMT testSkipping override (test: $name)")
    val jsonRecords = data.split("\n").toSeq
    val allConfs = sqlConfs ++ getDataSkippingConfs(indexedCols, deltaStatsColNamesOpt)

    def writeTable(t: String, overwrite: Boolean = false): DeltaLog = {
      val reader = spark.read
      if (schema != null) { reader.schema(schema) }
      val writer = reader.json(jsonRecords.toDS()).coalesce(1).write.format("delta")
      if (overwrite) { writer.mode("overwrite") }
      writer.saveAsTable(t)
      DeltaLog.forTable(spark, TableIdentifier(t))
    }

    test(s"data skipping by stats using AMT checkpoint - $name") {
      withSQLConf(allConfs: _*) {
        val t = "ds_amt_checkpoint"
        withTable(t) {
          val r = writeTable(t)
          val log = checkpointAndCreateNewLogIfNecessary(r)
          assertAMTV1(log)
          checkSkipping(log, hits, misses, data, checkEmptyUnusedFiltersForHits)
        }
      }
    }

    test(s"data skipping by stats using AMT checkpoint and delta removes - $name") {
      withSQLConf(allConfs: _*) {
        val t = "ds_amt_checkpoint_and_delta_removes"
        withTable(t) {
          val r = writeTable(t)
          checkpointAndCreateNewLogIfNecessary(r)
          // Overwrite with the same data: the delta on top removes the manifest's files and re-adds
          // identical ones, so log replay must merge manifest AddFiles with the delta's
          // RemoveFile/AddFile actions. The data (hence hits/misses) is unchanged.
          writeTable(t, overwrite = true)
          assertAMTV1(r)
          checkSkipping(r, hits, misses, data, checkEmptyUnusedFiltersForHits)
        }
      }
    }
  }
}

/**
 * The base DataSkipping corpus on AMT tables (V1 reader).
 */
trait DataSkippingDeltaV1AMTTests
  extends DataSkippingDeltaV1Tests
  with DataSkippingAMTBase
  with DataSkippingDeltaTestV1ColumnMappingMode
