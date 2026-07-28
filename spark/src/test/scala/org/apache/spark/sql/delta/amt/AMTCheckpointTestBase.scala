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

package org.apache.spark.sql.delta.amt

import java.io.File

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{AdaptiveMetadataTableFeature, DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, Checkpoint}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col

/**
 * Shared fixtures for AMT (`adaptiveMetadata-preview`) test suites: table creation, manifest-tree
 * file lookups, and typed access to the snapshot's [[AMTCheckpointProvider]].
 *
 * AMT requires the `catalogManaged` feature, so tables must be catalog-managed and accessed by
 * name (path-based access is blocked). This mixes in [[CatalogOwnedTestBaseSuite]] to register an
 * in-memory commit coordinator and creates/accesses tables by name.
 */
trait AMTCheckpointTestBase
  extends QueryTest
  with CatalogOwnedTestBaseSuite
  with DeltaSQLCommandTest {

  // Register the in-memory commit coordinator so catalog-managed AMT tables can be created locally.
  // Backfill batch size 1 so every commit is backfilled to a standard NNN.json immediately (rather
  // than staying as a UUID-named staged commit); the suites read commit actions via
  // `deltaLog.getChanges`, which only sees backfilled deltas.
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key, "false")

  /** Typed view of a snapshot's checkpoint provider when it is AMT-backed. */
  protected def amtProvider(snapshot: Snapshot): Option[AMTCheckpointProvider] =
    snapshot.checkpointProvider match {
      case amt: AMTCheckpointProvider => Some(amt)
      case _ => None
    }

  /** The [[DeltaLog]] for a catalog-managed table accessed by name. */
  protected def deltaLogForName(tableName: String): DeltaLog =
    DeltaLog.forTable(spark, new TableIdentifier(tableName))

  /** The physical data path of a catalog-managed table accessed by name. */
  protected def tablePath(tableName: String): String =
    new File(deltaLogForName(tableName).dataPath.toUri).getCanonicalPath

  protected def createAMTTable(tableName: String, checkpointInterval: Int = 2): Unit = {
    sql(
      s"""CREATE TABLE $tableName (id INT) USING DELTA
         |TBLPROPERTIES (
         |  '${propertyKey(AdaptiveMetadataTableFeature)}' = '$FEATURE_PROP_SUPPORTED',
         |  'delta.columnMapping.mode' = 'id',
         |  'delta.enableDeletionVectors' = 'true',
         |  'delta.checkpointInterval' = '$checkpointInterval')""".stripMargin)
  }

  /**
   * How an AMT is emitted for a triggering commit. In [[AMTWriteMode.Inline]] the manifest tree
   * rides in the business commit itself; in [[AMTWriteMode.Deferred]] a follow-up OPTIMIZE
   * CHECKPOINT commit (issued by the post-commit hook) lands it one version later.
   */
  protected sealed trait AMTWriteMode {
    /** Number of extra commits the AMT emission adds after a triggering business commit. */
    def followUpCommits: Int
  }
  protected object AMTWriteMode {
    case object Inline extends AMTWriteMode { val followUpCommits = 0 }
    case object Deferred extends AMTWriteMode { val followUpCommits = 1 }
  }

  /**
   * Registers a test in both AMT write modes. The body runs once with inline writes forced (a low
   * action-count threshold) and once with the default deferred follow-up-commit path, so behavior
   * is covered in both. The body receives the active [[AMTWriteMode]] for any version-relative
   * assertions.
   */
  protected def testInlineAndDeferred(testName: String)(body: AMTWriteMode => Unit): Unit = {
    test(s"$testName (inline)") {
      withSQLConf(
          DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key
            -> "1") {
        body(AMTWriteMode.Inline)
      }
    }
    test(s"$testName (deferred)") {
      // Default threshold (Long.MaxValue) keeps business commits from writing inline.
      body(AMTWriteMode.Deferred)
    }
  }

  /** True iff `name` looks like an AMT leaf parquet file. */
  protected def isLeafFileName(name: String): Boolean =
    name.startsWith("leaf-") && name.endsWith(".parquet")

  /** True iff `name` looks like an AMT root parquet file. */
  protected def isRootFileName(name: String): Boolean =
    name.startsWith("root-") && name.endsWith(".parquet")

  /** Lists the AMT manifest files under `<path>/metadata/` matching `predicate` on the name. */
  protected def metadataFiles(path: String, predicate: String => Boolean): Seq[File] = {
    val dir = new File(path, FileNames.AMT_METADATA_DIR_NAME)
    if (!dir.exists()) Seq.empty
    else Option(dir.listFiles()).toSeq.flatten.filter(f => predicate(f.getName))
  }

  protected def leafFiles(path: String): Seq[File] =
    metadataFiles(path, isLeafFileName)

  protected def rootFiles(path: String): Seq[File] =
    metadataFiles(path, isRootFileName)

  /** Returns the actions committed at exactly `version`. */
  protected def actionsAt(deltaLog: DeltaLog, version: Long): Seq[Action] =
    deltaLog.getChanges(version).find(_._1 == version).map(_._2).getOrElse(Seq.empty)

  protected def checkpointsAt(deltaLog: DeltaLog, version: Long): Seq[Checkpoint] =
    actionsAt(deltaLog, version).collect { case c: Checkpoint => c }

  /**
   * Total DATA (content_type=0) entry rows across the leaves reachable from the CURRENT snapshot's
   * manifest tree. Reads only the provider's leaves (not every file under `metadata/`, which
   * accumulates superseded leaves from earlier checkpoints).
   */
  protected def currentLeafDataEntries(snapshot: Snapshot): Long = {
    val provider = amtProvider(snapshot)
      .getOrElse(fail("Snapshot has no AMTCheckpointProvider."))
      provider.leaves.map { leaf =>
        spark.read.parquet(leaf.path)
          .where(col("content_type") === AMTSingleAction.ContentType.Type.Data)
          .count()
      }.sum
  }
}
