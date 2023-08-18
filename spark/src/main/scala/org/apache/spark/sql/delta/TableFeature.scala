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

import java.util.Locale

import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.constraints.{Constraints, Invariants}
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.TimestampNTZType

/* --------------------------------------- *
 |  Table features base class definitions  |
 * --------------------------------------- */

/**
 * A base class for all table features.
 *
 * A feature can be <b>explicitly supported</b> by a table's protocol when the protocol contains a
 * feature's `name`. Writers (for writer-only features) or readers and writers (for reader-writer
 * features) must recognize supported features and must handle them appropriately.
 *
 * A table feature that released before Delta Table Features (reader version 3 and writer version
 * 7) is considered as a <b>legacy feature</b>. Legacy features are <b>implicitly supported</b>
 * when (a) the protocol does not support table features, i.e., has reader version less than 3 or
 * writer version less than 7 and (b) the feature's minimum reader/writer version is less than or
 * equal to the current protocol's reader/writer version.
 *
 * Separately, a feature can be automatically supported by a table's metadata when certain
 * feature-specific table properties are set. For example, `changeDataFeed` is automatically
 * supported when there's a table property `delta.enableChangeDataFeed=true`. This is independent
 * of the table's enabled features. When a feature is supported (explicitly or implicitly) by the
 * table protocol but its metadata requirements are not satisfied, then clients still have to
 * understand the feature (at least to the extent that they can read and preserve the existing
 * data in the table that uses the feature). See the documentation of
 * [[FeatureAutomaticallyEnabledByMetadata]] for more information.
 *
 * @param name
 *   a globally-unique string indicator to represent the feature. All characters must be letters
 *   (a-z, A-Z), digits (0-9), '-', or '_'. Words must be in camelCase.
 * @param minReaderVersion
 *   the minimum reader version this feature requires. For a feature that can only be explicitly
 *   supported, this is either `0` or `3` (the reader protocol version that supports table
 *   features), depending on the feature is writer-only or reader-writer. For a legacy feature
 *   that can be implicitly supported, this is the first protocol version which the feature is
 *   introduced.
 * @param minWriterVersion
 *   the minimum writer version this feature requires. For a feature that can only be explicitly
 *   supported, this is the writer protocol `7` that supports table features. For a legacy feature
 *   that can be implicitly supported, this is the first protocol version which the feature is
 *   introduced.
 */
// @TODO: distinguish Delta and 3rd-party features and give appropriate error messages
sealed abstract class TableFeature(
    val name: String,
    val minReaderVersion: Int,
    val minWriterVersion: Int) extends java.io.Serializable {

  require(name.forall(c => c.isLetterOrDigit || c == '-' || c == '_'))

  /**
   * Get a [[Protocol]] object stating the minimum reader and writer versions this feature
   * requires. For a feature that can only be explicitly supported, this method returns a protocol
   * version that supports table features, either `(0,7)` or `(3,7)` depending on the feature is
   * writer-only or reader-writer. For a legacy feature that can be implicitly supported, this
   * method returns the first protocol version which introduced the said feature.
   *
   * For all features, if the table's protocol version does not support table features, then the
   * minimum protocol version is enough. However, if the protocol version supports table features
   * for the feature type (writer-only or reader-writer), then the minimum protocol version is not
   * enough to support a feature. In this case the feature must also be explicitly listed in the
   * appropriate feature sets in the [[Protocol]].
   */
  def minProtocolVersion: Protocol = Protocol(minReaderVersion, minWriterVersion)

  /** Determine if this feature applies to both readers and writers. */
  def isReaderWriterFeature: Boolean = this.isInstanceOf[ReaderWriterFeatureType]

  /**
   * Determine if this feature is a legacy feature. See the documentation of [[TableFeature]] for
   * more information.
   */
  def isLegacyFeature: Boolean = this.isInstanceOf[LegacyFeatureType]

  /**
   * True if this feature can be removed.
   */
  def isRemovable: Boolean = this.isInstanceOf[RemovableFeature]

  /**
   * Set of table features that this table feature depends on. I.e. the set of features that need
   * to be enabled if this table feature is enabled.
   */
  def requiredFeatures: Set[TableFeature] = Set.empty
}

/** A trait to indicate a feature applies to readers and writers. */
sealed trait ReaderWriterFeatureType

/** A trait to indicate a feature is legacy, i.e., released before Table Features. */
sealed trait LegacyFeatureType

/**
 * A trait indicating this feature can be automatically enabled via a change in a table's
 * metadata, e.g., through setting particular values of certain feature-specific table properties.
 *
 * When the feature's metadata requirements are satisfied for <b>new tables</b>, or for
 * <b>existing tables when [[automaticallyUpdateProtocolOfExistingTables]] set to `true`</b>, the
 * client will silently add the feature to the protocol's `readerFeatures` and/or
 * `writerFeatures`. Otherwise, a proper protocol version bump must be present in the same
 * transaction.
 */
sealed trait FeatureAutomaticallyEnabledByMetadata { this: TableFeature =>

  /**
   * Whether the feature can automatically update the protocol of an existing table when the
   * metadata requirements are satisfied. As a rule of thumb, a table feature that requires
   * explicit operations (e.g., turning on a table property) should set this flag to `true`, while
   * features that are used implicitly (e.g., when using a new data type) should set this flag to
   * `false`.
   */
  def automaticallyUpdateProtocolOfExistingTables: Boolean = this.isLegacyFeature

  /**
   * Determine whether the feature must be supported and enabled because its metadata requirements
   * are satisfied.
   */
  def metadataRequiresFeatureToBeEnabled(metadata: Metadata, spark: SparkSession): Boolean

  require(
    !this.isLegacyFeature || automaticallyUpdateProtocolOfExistingTables,
    "Legacy feature must be auto-update capable.")
}

/**
 * A trait indicating a feature can be removed. Classes that extend the trait need to
 * implement the following three functions:
 *
 * a) preDowngradeCommand. This is where all required actions for removing the feature are
 *    implemented. For example, to remove the DVs feature we need to remove metadata config
 *    and purge all DVs from table. This action takes place before the protocol downgrade in
 *    separate commit(s). Note, the command needs to be implemented in a way concurrent
 *    transactions do not nullify the effect. For example, disabling DVs on a table before
 *    purging will stop concurrent transactions from adding DVs. During protocol downgrade
 *    we perform a validation in [[validateRemoval]] to make sure all invariants still hold.
 *
 * b) validateRemoval. Add any feature-specific checks before proceeding to the protocol
 *    downgrade. This function is guaranteed to be called at the latest version before the
 *    protocol downgrade is committed to the table. When the protocol downgrade txn conflicts,
 *    the validation is repeated against the winning txn snapshot. As soon as the protocol
 *    downgrade succeeds, all subsequent interleaved txns are aborted.
 *
 * c) actionUsesFeature. For reader+writer features we check whether past versions contain any
 *    traces of the removed feature. This is achieved by calling [[actionUsesFeature]] for
 *    every action of every reachable commit version in the log. Note, a feature may leave traces
 *    in both data and metadata. Depending on the feature, we need to check several types of
 *    actions such as Metadata, AddFile, RemoveFile etc.
 *    Writer features should directly return false.
 *
 *    WARNING: actionUsesFeature should not check Protocol actions for the feature being removed,
 *    because at the time actionUsesFeature is invoked the protocol downgrade did not happen yet.
 *    Thus, the feature-to-remove is still active. As a result, any unrelated operations that
 *    produce a protocol action (while we are waiting for the retention period to expire) will
 *    "carry" the feature-to-remove. Checking protocol for that feature would result in an
 *    unnecessary failure during the history validation of the next DROP FEATURE call. Note,
 *    while the feature-to-remove is supported in the protocol we cannot generate a legit protocol
 *    action that adds support for that feature since it is already supported.
 */
sealed trait RemovableFeature { self: TableFeature =>
  def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand
  def validateRemoval(snapshot: Snapshot): Boolean
  def actionUsesFeature(action: Action): Boolean

  /**
   * Examines all historical commits for traces of the removableFeature.
   * This is achieved as follows:
   *
   * 1) We find the earliest valid checkpoint, recreate a snapshot at that version and we check
   *    whether there any traces of the feature-to-remove.
   * 2) We check all commits that exist between version 0 and the current version.
   *    This includes the versions we validated the snapshots. This is because a commit
   *    might include information that is not available in the snapshot. Examples include
   *    CommitInfo, CDCInfo etc. Note, there can still be valid log commit files with
   *    versions prior the earliest checkpoint version.
   * 3) We do not need to recreate a snapshot at the current version because this is already being
   *    handled by validateRemoval.
   *
   * Note, this is a slow process.
   *
   * @param spark The SparkSession.
   * @param downgradeTxnReadSnapshot The read snapshot of the protocol downgrade transaction.
   * @return True if the history contains any trace of the feature.
   */
  def historyContainsFeature(
      spark: SparkSession,
      downgradeTxnReadSnapshot: Snapshot): Boolean = {
    require(isReaderWriterFeature)
    val deltaLog = downgradeTxnReadSnapshot.deltaLog
    val earliestCheckpointVersion = deltaLog.findEarliestReliableCheckpoint().getOrElse(0L)
    val toVersion = downgradeTxnReadSnapshot.version

    // Use the snapshot at earliestCheckpointVersion to validate the checkpoint identified by
    // findEarliestReliableCheckpoint.
    val earliestSnapshot = deltaLog.getSnapshotAt(earliestCheckpointVersion)
    if (containsFeatureTraces(earliestSnapshot.stateDS)) {
      return true
    }

    // Check if commits between 0 version and toVersion contain any traces of the feature.
    val allHistoricalDeltaFiles = deltaLog
      .listFrom(0L)
      .takeWhile(file => FileNames.getFileVersionOpt(file.getPath).forall(_ <= toVersion))
      .filter(FileNames.isDeltaFile)
      .toSeq
    DeltaLogFileIndex(DeltaLogFileIndex.COMMIT_FILE_FORMAT, allHistoricalDeltaFiles)
      .exists(i => containsFeatureTraces(deltaLog.loadIndex(i, Action.logSchema).as[SingleAction]))
  }

  /** Returns whether a dataset of actions contains any trace of this feature. */
  private def containsFeatureTraces(ds: Dataset[SingleAction]): Boolean = {
    import org.apache.spark.sql.delta.implicits._
    ds.mapPartitions { actions =>
      actions
        .map(_.unwrap)
        .collectFirst { case a if actionUsesFeature(a) => true }
        .toIterator
    }.take(1).nonEmpty
  }
}

/**
 * A base class for all writer-only table features that can only be explicitly supported.
 *
 * @param name
 *   a globally-unique string indicator to represent the feature. All characters must be letters
 *   (a-z, A-Z), digits (0-9), '-', or '_'. Words must be in camelCase.
 */
sealed abstract class WriterFeature(name: String)
  extends TableFeature(
    name,
    minReaderVersion = 0,
    minWriterVersion = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)

/**
 * A base class for all reader-writer table features that can only be explicitly supported.
 *
 * @param name
 *   a globally-unique string indicator to represent the feature. All characters must be letters
 *   (a-z, A-Z), digits (0-9), '-', or '_'. Words must be in camelCase.
 */
sealed abstract class ReaderWriterFeature(name: String)
  extends WriterFeature(name)
  with ReaderWriterFeatureType {
  override val minReaderVersion: Int = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_READER_VERSION
}

/**
 * A base class for all table legacy writer-only features.
 *
 * @param name
 *   a globally-unique string indicator to represent the feature. Allowed characters are letters
 *   (a-z, A-Z), digits (0-9), '-', and '_'. Words must be in camelCase.
 * @param minWriterVersion
 *   the minimum writer protocol version that supports this feature.
 */
sealed abstract class LegacyWriterFeature(name: String, minWriterVersion: Int)
  extends TableFeature(name, minReaderVersion = 0, minWriterVersion = minWriterVersion)
  with LegacyFeatureType

/**
 * A base class for all legacy writer-only table features.
 *
 * @param name
 *   a globally-unique string indicator to represent the feature. Allowed characters are letters
 *   (a-z, A-Z), digits (0-9), '-', and '_'. Words must be in camelCase.
 * @param minReaderVersion
 *   the minimum reader protocol version that supports this feature.
 * @param minWriterVersion
 *   the minimum writer protocol version that supports this feature.
 */
sealed abstract class LegacyReaderWriterFeature(
    name: String,
    override val minReaderVersion: Int,
    minWriterVersion: Int)
  extends LegacyWriterFeature(name, minWriterVersion)
  with ReaderWriterFeatureType

object TableFeature {
  /**
   * All table features recognized by this client. Update this set when you added a new Table
   * Feature.
   *
   * Warning: Do not call `get` on this Map to get a specific feature because keys in this map are
   * in lower cases. Use [[featureNameToFeature]] instead.
   */
  private[delta] val allSupportedFeaturesMap: Map[String, TableFeature] = {
    var features: Set[TableFeature] = Set(
      AppendOnlyTableFeature,
      ChangeDataFeedTableFeature,
      CheckConstraintsTableFeature,
      DomainMetadataTableFeature,
      GeneratedColumnsTableFeature,
      InvariantsTableFeature,
      ColumnMappingTableFeature,
      TimestampNTZTableFeature,
      IcebergCompatV1TableFeature,
      DeletionVectorsTableFeature,
      V2CheckpointTableFeature)
    if (DeltaUtils.isTesting) {
      features ++= Set(
        TestLegacyWriterFeature,
        TestLegacyReaderWriterFeature,
        TestWriterFeature,
        TestWriterMetadataNoAutoUpdateFeature,
        TestReaderWriterFeature,
        TestReaderWriterMetadataAutoUpdateFeature,
        TestReaderWriterMetadataNoAutoUpdateFeature,
        TestRemovableWriterFeature,
        TestRemovableLegacyWriterFeature,
        TestRemovableReaderWriterFeature,
        TestRemovableLegacyReaderWriterFeature,
        TestFeatureWithDependency,
        TestFeatureWithTransitiveDependency,
        TestWriterFeatureWithTransitiveDependency,
        // Row IDs are still under development and only available in testing.
        RowTrackingFeature)
    }
    val exposeV2Checkpoints =
      DeltaUtils.isTesting || SparkSession.getActiveSession.exists { spark =>
        spark.conf.get(DeltaSQLConf.EXPOSE_CHECKPOINT_V2_TABLE_FEATURE_FOR_TESTING)
      }
    if (exposeV2Checkpoints) {
      features += V2CheckpointTableFeature
    }
    val featureMap = features.map(f => f.name.toLowerCase(Locale.ROOT) -> f).toMap
    require(features.size == featureMap.size, "Lowercase feature names must not duplicate.")
    featureMap
  }

  /** Get a [[TableFeature]] object by its name. */
  def featureNameToFeature(featureName: String): Option[TableFeature] =
    allSupportedFeaturesMap.get(featureName.toLowerCase(Locale.ROOT))

  /**
   * Extracts the removed (explicit) feature names by comparing new and old protocols.
   * Returns None if there are no removed (explicit) features.
   */
  protected def getDroppedExplicitFeatureNames(
      newProtocol: Protocol,
      oldProtocol: Protocol): Option[Set[String]] = {
    val newFeatureNames = newProtocol.readerAndWriterFeatureNames
    val oldFeatureNames = oldProtocol.readerAndWriterFeatureNames
    Option(oldFeatureNames -- newFeatureNames).filter(_.nonEmpty)
  }

  /**
   * Identifies whether there was any feature removal between two protocols.
   */
  def isProtocolRemovingExplicitFeatures(newProtocol: Protocol, oldProtocol: Protocol): Boolean = {
    getDroppedExplicitFeatureNames(newProtocol = newProtocol, oldProtocol = oldProtocol).isDefined
  }

  /**
   * Validates whether all requirements of a removed feature hold against the provided snapshot.
   */
  def validateFeatureRemovalAtSnapshot(
      newProtocol: Protocol,
      oldProtocol: Protocol,
      snapshot: Snapshot): Boolean = {
    val droppedFeatureNamesOpt = TableFeature.getDroppedExplicitFeatureNames(
      newProtocol = newProtocol,
      oldProtocol = oldProtocol)
    val droppedFeatureName = droppedFeatureNamesOpt match {
      case Some(f) if f.size == 1 => f.head
      // We do not support dropping more than one features at a time so we have to reject
      // the validation.
      case Some(_) => return false
      case None => return true
    }

    TableFeature.featureNameToFeature(droppedFeatureName) match {
      case Some(feature: RemovableFeature) => feature.validateRemoval(snapshot)
      case _ => throw DeltaErrors.dropTableFeatureFeatureNotSupportedByClient(droppedFeatureName)
    }
  }
}

/* ---------------------------------------- *
 |  All table features known to the client  |
 * ---------------------------------------- */

object AppendOnlyTableFeature
  extends LegacyWriterFeature(name = "appendOnly", minWriterVersion = 2)
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    DeltaConfigs.IS_APPEND_ONLY.fromMetaData(metadata)
  }
}

object InvariantsTableFeature
  extends LegacyWriterFeature(name = "invariants", minWriterVersion = 2)
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    Invariants.getFromSchema(metadata.schema, spark).nonEmpty
  }
}

object CheckConstraintsTableFeature
  extends LegacyWriterFeature(name = "checkConstraints", minWriterVersion = 3)
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    Constraints.getCheckConstraints(metadata, spark).nonEmpty
  }
}

object ChangeDataFeedTableFeature
  extends LegacyWriterFeature(name = "changeDataFeed", minWriterVersion = 4)
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    DeltaConfigs.CHANGE_DATA_FEED.fromMetaData(metadata)
  }
}

object GeneratedColumnsTableFeature
  extends LegacyWriterFeature(name = "generatedColumns", minWriterVersion = 4)
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    GeneratedColumn.hasGeneratedColumns(metadata.schema)
  }
}

object ColumnMappingTableFeature
  extends LegacyReaderWriterFeature(
    name = "columnMapping",
    minReaderVersion = 2,
    minWriterVersion = 5)
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.columnMappingMode match {
      case NoMapping => false
      case _ => true
    }
  }
}

object TimestampNTZTableFeature extends ReaderWriterFeature(name = "timestampNtz")
    with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata, spark: SparkSession): Boolean = {
    SchemaUtils.checkForTimestampNTZColumnsRecursively(metadata.schema)
  }
}

object DeletionVectorsTableFeature
  extends ReaderWriterFeature(name = "deletionVectors")
  with FeatureAutomaticallyEnabledByMetadata {
  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.fromMetaData(metadata)
  }
}

object RowTrackingFeature extends WriterFeature(name = "rowTracking")
  with FeatureAutomaticallyEnabledByMetadata {
  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = DeltaConfigs.ROW_TRACKING_ENABLED.fromMetaData(metadata)

  override def requiredFeatures: Set[TableFeature] = Set(DomainMetadataTableFeature)
}

object DomainMetadataTableFeature extends WriterFeature(name = "domainMetadata")

object IcebergCompatV1TableFeature extends WriterFeature(name = "icebergCompatV1")
  with FeatureAutomaticallyEnabledByMetadata {

  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = IcebergCompatV1.isEnabled(metadata)

  override def requiredFeatures: Set[TableFeature] = Set(ColumnMappingTableFeature)
}


/**
 * V2 Checkpoint table feature is for checkpoints with sidecars and the new format and
 * file naming scheme.
 * This is still WIP feature.
 */
object V2CheckpointTableFeature
  extends ReaderWriterFeature(name = "v2Checkpoint-under-development")
  with FeatureAutomaticallyEnabledByMetadata {

  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    DeltaConfigs.CHECKPOINT_POLICY.fromMetaData(metadata).needsV2CheckpointSupport
  }
}

/**
 * Features below are for testing only, and are being registered to the system only in the testing
 * environment. See [[TableFeature.allSupportedFeaturesMap]] for the registration.
 */

object TestLegacyWriterFeature
  extends LegacyWriterFeature(name = "testLegacyWriter", minWriterVersion = 5)

object TestWriterFeature extends WriterFeature(name = "testWriter")

object TestWriterMetadataNoAutoUpdateFeature
  extends WriterFeature(name = "testWriterMetadataNoAutoUpdate")
  with FeatureAutomaticallyEnabledByMetadata {
  val TABLE_PROP_KEY = "_123testWriterMetadataNoAutoUpdate321_"
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }
}

object TestLegacyReaderWriterFeature
  extends LegacyReaderWriterFeature(
    name = "testLegacyReaderWriter",
    minReaderVersion = 2,
    minWriterVersion = 5)

object TestReaderWriterFeature extends ReaderWriterFeature(name = "testReaderWriter")

object TestReaderWriterMetadataNoAutoUpdateFeature
  extends ReaderWriterFeature(name = "testReaderWriterMetadataNoAutoUpdate")
  with FeatureAutomaticallyEnabledByMetadata {
  val TABLE_PROP_KEY = "_123testReaderWriterMetadataNoAutoUpdate321_"
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }
}

object TestReaderWriterMetadataAutoUpdateFeature
  extends ReaderWriterFeature(name = "testReaderWriterMetadataAutoUpdate")
  with FeatureAutomaticallyEnabledByMetadata {
  val TABLE_PROP_KEY = "_123testReaderWriterMetadataAutoUpdate321_"

  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }
}

private[sql] object TestRemovableWriterFeature
  extends WriterFeature(name = "testRemovableWriter")
  with FeatureAutomaticallyEnabledByMetadata
  with RemovableFeature {

  val TABLE_PROP_KEY = "_123TestRemovableWriter321_"
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }

  override def validateRemoval(snapshot: Snapshot): Boolean =
    !snapshot.metadata.configuration.contains(TABLE_PROP_KEY)

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    TestWriterFeaturePreDowngradeCommand(table)

  override def actionUsesFeature(action: Action): Boolean = false
}

private[sql] object TestRemovableReaderWriterFeature
  extends ReaderWriterFeature(name = "testRemovableReaderWriter")
    with FeatureAutomaticallyEnabledByMetadata
    with RemovableFeature {

  val TABLE_PROP_KEY = "_123TestRemovableReaderWriter321_"
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }

  override def validateRemoval(snapshot: Snapshot): Boolean =
    !snapshot.metadata.configuration.contains(TABLE_PROP_KEY)

  override def actionUsesFeature(action: Action): Boolean = action match {
    case m: Metadata => m.configuration.contains(TABLE_PROP_KEY)
    case _ => false
  }

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    TestReaderWriterFeaturePreDowngradeCommand(table)
}

object TestRemovableLegacyWriterFeature
  extends LegacyWriterFeature(name = "testRemovableLegacyWriter", minWriterVersion = 5)
  with FeatureAutomaticallyEnabledByMetadata
  with RemovableFeature {

  val TABLE_PROP_KEY = "_123TestRemovableLegacyWriter321_"
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }

  override def validateRemoval(snapshot: Snapshot): Boolean = {
    !snapshot.metadata.configuration.contains(TABLE_PROP_KEY)
  }

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    TestLegacyWriterFeaturePreDowngradeCommand(table)

  override def actionUsesFeature(action: Action): Boolean = false
}

object TestRemovableLegacyReaderWriterFeature
  extends LegacyReaderWriterFeature(
      name = "testRemovableLegacyReaderWriter", minReaderVersion = 2, minWriterVersion = 5)
  with FeatureAutomaticallyEnabledByMetadata
  with RemovableFeature {

  val TABLE_PROP_KEY = "_123TestRemovableLegacyReaderWriter321_"
  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }

  override def validateRemoval(snapshot: Snapshot): Boolean = {
    !snapshot.metadata.configuration.contains(TABLE_PROP_KEY)
  }

  override def actionUsesFeature(action: Action): Boolean = {
    action match {
      case m: Metadata => m.configuration.contains(TABLE_PROP_KEY)
      case _ => false
    }
  }

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    TestLegacyReaderWriterFeaturePreDowngradeCommand(table)
}

object TestFeatureWithDependency
  extends ReaderWriterFeature(name = "testFeatureWithDependency")
  with FeatureAutomaticallyEnabledByMetadata {

  val TABLE_PROP_KEY = "_123testFeatureWithDependency321_"

  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      metadata: Metadata, spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }

  override def requiredFeatures: Set[TableFeature] = Set(TestReaderWriterFeature)
}

object TestFeatureWithTransitiveDependency
  extends ReaderWriterFeature(name = "testFeatureWithTransitiveDependency") {

  override def requiredFeatures: Set[TableFeature] = Set(TestFeatureWithDependency)
}

object TestWriterFeatureWithTransitiveDependency
  extends WriterFeature(name = "testWriterFeatureWithTransitiveDependency") {

  override def requiredFeatures: Set[TableFeature] = Set(TestFeatureWithDependency)
}
