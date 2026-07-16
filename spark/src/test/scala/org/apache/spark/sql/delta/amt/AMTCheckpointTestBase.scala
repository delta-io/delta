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
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Shared fixtures for AMT (`adaptiveMetadata-preview`) test suites: table creation, manifest-tree
 * file lookups, and typed access to the snapshot's [[AMTCheckpointProvider]].
 */
trait AMTCheckpointTestBase
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key, "false")

  /** Typed view of a snapshot's checkpoint provider when it is AMT-backed. */
  protected def amtProvider(snapshot: Snapshot): Option[AMTCheckpointProvider] =
    snapshot.checkpointProvider match {
      case amt: AMTCheckpointProvider => Some(amt)
      case _ => None
    }

  protected def createAMTTable(path: String, checkpointInterval: Int = 2): Unit = {
    sql(
      s"""CREATE TABLE delta.`$path` (id INT) USING DELTA
         |TBLPROPERTIES (
         |  '${propertyKey(AdaptiveMetadataTableFeature)}' = '$FEATURE_PROP_SUPPORTED',
         |  'delta.columnMapping.mode' = 'name',
         |  'delta.enableDeletionVectors' = 'true',
         |  'delta.checkpointInterval' = '$checkpointInterval')""".stripMargin)
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
