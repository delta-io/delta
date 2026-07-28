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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql.SparkSession

object InCommitTimestampUtils {

  /** Pairs a commit version with the [[Metadata]] that was written at that version. */
  case class MetadataWithVersion(version: Long, metadata: Metadata)

  final val TABLE_PROPERTY_CONFS = Seq(
    DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED,
    DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
    DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP)

  final val TABLE_PROPERTY_KEYS: Seq[String] = TABLE_PROPERTY_CONFS.map(_.key)

  /**
   * Returns true if ICT was newly enabled by the commit at [[currentMetadataWithVersion.version]],
   * i.e. it is enabled there but was not enabled in the immediately preceding version.
   *
   * WARNING: The Metadata() of InitialSnapshot can have ICT=true from the default table
   * property. When priorMetadataWithVersion.version is -1 (no prior commit exists),
   * we treat ICT as not previously enabled so that a genuine first-commit enablement
   * returns true.
   *
   * Note: [[priorMetadataWithVersion]] may be an approximation of the metadata at the
   * preceding version (e.g. the read snapshot's metadata when the winning commit had no
   * metadata update). Callers are responsible for ensuring the value correctly reflects
   * the ICT state at the preceding version for their use case.
   */
  def didCurrentTransactionEnableICT(
      currentMetadataWithVersion: MetadataWithVersion,
      priorMetadataWithVersion: MetadataWithVersion): Boolean = {
    assert(currentMetadataWithVersion.version == priorMetadataWithVersion.version + 1)
    val isICTCurrentlyEnabled =
      DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(currentMetadataWithVersion.metadata)
    val wasICTPreviouslyEnabled = priorMetadataWithVersion.version != -1 &&
      DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(
        priorMetadataWithVersion.metadata)
    isICTCurrentlyEnabled && !wasICTPreviouslyEnabled
  }

  /**
   * Returns the updated [[Metadata]] with inCommitTimestamp enablement related info
   * (version and timestamp) correctly set.
   * This enablement info will be set to the current commit's timestamp and version if:
   * 1. If this transaction enables inCommitTimestamp.
   * 2. If the commit version is not 0. This is because we only need to persist
   *  the enablement info if there are non-ICT commits in the Delta log.
   * For cases where ICT is enabled in both the current transaction and the preceding version,
   * we will retain the enablement info from the preceding version. Note that this can
   * happen for commands like REPLACE or CLONE, where we can end up dropping the enablement
   * info due to the belief that ICT was just enabled.
   * Note: This function must only be called after transaction conflicts have been resolved.
   *
   * @param currentMetadataWithVersion the version and metadata being committed.
   * @param priorMetadataWithVersion the version and metadata of the immediately preceding
   *   committed version. This can be from the read snapshot of the current transaction or from a
   *   winning commit on top of which the current transaction is rebased.
   */
  def getUpdatedMetadataWithICTEnablementInfo(
      spark: SparkSession,
      inCommitTimestamp: Long,
      currentMetadataWithVersion: MetadataWithVersion,
      priorMetadataWithVersion: MetadataWithVersion): Option[Metadata] = {
    val commitVersion = currentMetadataWithVersion.version
    val metadata = currentMetadataWithVersion.metadata
    assert(
      commitVersion == priorMetadataWithVersion.version + 1,
      s"In-commit timestamp enablement tracking requires consecutive versions " +
      s"(got commitVersion=$commitVersion, priorVersion=" +
      s"${priorMetadataWithVersion.version})")
    val currentTxnEnabledICT =
      didCurrentTransactionEnableICT(currentMetadataWithVersion, priorMetadataWithVersion)
    if (currentTxnEnabledICT && commitVersion != 0) {
      val enablementTrackingProperties = Map(
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.key -> commitVersion.toString,
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.key -> inCommitTimestamp.toString)
      Some(metadata.copy(configuration = metadata.configuration ++ enablementTrackingProperties))
    } else if (DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(metadata) &&
        !currentTxnEnabledICT &&
        // This check ensures that we don't make an unnecessary metadata update
        // even when ICT enablement properties are not being dropped.
        getValidatedICTEnablementInfo(priorMetadataWithVersion.metadata).isDefined &&
        getValidatedICTEnablementInfo(metadata).isEmpty &&
        spark.conf.get(DeltaSQLConf.IN_COMMIT_TIMESTAMP_RETAIN_ENABLEMENT_INFO_FIX_ENABLED)
    ) {
      // If ICT was enabled in the preceding version and is still enabled, retain the
      // enablement info. This prevents it from being dropped during REPLACE/CLONE.
      val existingICTConfigs = priorMetadataWithVersion.metadata.configuration
        .filter { case (k, _) => TABLE_PROPERTY_KEYS.contains(k) }
      Some(metadata.copy(configuration = metadata.configuration ++ existingICTConfigs))
    } else {
      None
    }
  }

  def getValidatedICTEnablementInfo(metadata: Metadata): Option[DeltaHistoryManager.Commit] = {
    val enablementTimestampOpt =
      DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetaData(metadata)
    val enablementVersionOpt =
      DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetaData(metadata)
    (enablementTimestampOpt, enablementVersionOpt) match {
      case (Some(enablementTimestamp), Some(enablementVersion)) =>
        Some(DeltaHistoryManager.Commit(enablementVersion, enablementTimestamp))
      case (None, None) =>
        None
      case _ =>
        throw new IllegalStateException(
          "Both enablement version and timestamp should be present or absent together.")
    }
  }
}
