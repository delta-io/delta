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
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.constraints.{Constraints, Invariants}
import org.apache.spark.sql.delta.coordinatedcommits.CoordinatedCommitsUtils
import org.apache.spark.sql.delta.redirect.{RedirectReaderWriter, RedirectWriterOnly}
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
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
  def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol, metadata: Metadata, spark: SparkSession): Boolean

  require(
    !this.isLegacyFeature || automaticallyUpdateProtocolOfExistingTables,
    "Legacy feature must be auto-update capable.")
}

/**
 * A trait indicating a feature can be removed. Classes that extend the trait need to
 * implement the following four functions:
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
 *    The implementation should return true if there are no feature traces in the latest
 *    version. False otherwise.
 *
 * c) requiresHistoryProtection. It indicates whether the feature leaves traces in the table
 *    history that may result in incorrect behaviour if the table is read/written by a client
 *    that does not support the feature. This is by default true for all reader+writer features
 *    and false for writer features.
 *    WARNING: Disabling [[requiresHistoryProtection]] for relevant features could result in
 *    incorrect snapshot reconstruction.
 *
 * d) actionUsesFeature. For features that require history truncation we verify whether past
 *    versions contain any traces of the removed feature. This is achieved by calling
 *    [[actionUsesFeature]] for every action of every reachable commit version in the log.
 *    Note, a feature may leave traces in both data and metadata. Depending on the feature, we
 *    need to check several types of actions such as Metadata, AddFile, RemoveFile etc.
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
  def requiresHistoryProtection: Boolean = isReaderWriterFeature
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
    require(requiresHistoryProtection)
    val deltaLog = downgradeTxnReadSnapshot.deltaLog
    val earliestCheckpointVersion = deltaLog.findEarliestReliableCheckpoint.getOrElse(0L)
    val toVersion = downgradeTxnReadSnapshot.version

    // Use the snapshot at earliestCheckpointVersion to validate the checkpoint identified by
    // findEarliestReliableCheckpoint.
    val earliestSnapshot = deltaLog.getSnapshotAt(earliestCheckpointVersion)

    // Tombstones may contain traces of the removed feature. The earliest snapshot will include
    // all tombstones within the tombstoneRetentionPeriod. This may disallow protocol downgrade
    // because the log retention period is not aligned with the tombstoneRetentionPeriod.
    // To resolve this issue, we filter out all tombstones from the earliest checkpoint.
    // Tombstones at the earliest checkpoint should be irrelevant and should not be an
    // issue for readers that do not support the feature.
    if (containsFeatureTraces(earliestSnapshot.stateDS.filter("remove is null"))) {
      return true
    }

    // Check if commits between 0 version and toVersion contain any traces of the feature.
    val allHistoricalDeltaFiles = deltaLog
      .getChangeLogFiles(0)
      .takeWhile { case (version, _) => version <= toVersion }
      .map { case (_, file) => file }
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
  val isTesting = DeltaUtils.isTesting

  /**
   * All table features recognized by this client. Update this set when you added a new Table
   * Feature.
   *
   * Warning: Do not call `get` on this Map to get a specific feature because keys in this map are
   * in lower cases. Use [[featureNameToFeature]] instead.
   */
  def allSupportedFeaturesMap: Map[String, TableFeature] = {
    val testingFeaturesEnabled =
      try {
        SparkSession
        .getActiveSession
        .map(_.conf.get(DeltaSQLConf.TABLE_FEATURES_TEST_FEATURES_ENABLED))
        .getOrElse(true)
      } catch {
          case _ => true
      }
   var features: Set[TableFeature] = Set(
      AllowColumnDefaultsTableFeature,
      AppendOnlyTableFeature,
      ChangeDataFeedTableFeature,
      CheckConstraintsTableFeature,
      ClusteringTableFeature,
      DomainMetadataTableFeature,
      GeneratedColumnsTableFeature,
      IdentityColumnsTableFeature,
      InvariantsTableFeature,
      ColumnMappingTableFeature,
      TimestampNTZTableFeature,
      TypeWideningPreviewTableFeature,
      TypeWideningTableFeature,
      IcebergCompatV1TableFeature,
      IcebergCompatV2TableFeature,
      DeletionVectorsTableFeature,
      VacuumProtocolCheckTableFeature,
      V2CheckpointTableFeature,
      RowTrackingFeature,
      InCommitTimestampTableFeature,
      VariantTypePreviewTableFeature,
      VariantTypeTableFeature,
      CoordinatedCommitsTableFeature,
      CheckpointProtectionTableFeature)
    if (isTesting && testingFeaturesEnabled) {
      features ++= Set(
        RedirectReaderWriterFeature,
        RedirectWriterOnlyFeature,
        TestLegacyWriterFeature,
        TestLegacyReaderWriterFeature,
        TestWriterFeature,
        TestWriterMetadataNoAutoUpdateFeature,
        TestReaderWriterFeature,
        TestUnsupportedReaderWriterFeature,
        TestReaderWriterMetadataAutoUpdateFeature,
        TestReaderWriterMetadataNoAutoUpdateFeature,
        TestRemovableWriterFeature,
        TestRemovableWriterFeatureWithDependency,
        TestRemovableWriterWithHistoryTruncationFeature,
        TestRemovableLegacyWriterFeature,
        TestRemovableReaderWriterFeature,
        TestRemovableLegacyReaderWriterFeature,
        TestFeatureWithDependency,
        TestFeatureWithTransitiveDependency,
        TestWriterFeatureWithTransitiveDependency)
    }
    val featureMap = features.map(f => f.name.toLowerCase(Locale.ROOT) -> f).toMap
    require(features.size == featureMap.size, "Lowercase feature names must not duplicate.")
    featureMap
  }

  /** Test only features that appear unsupported in order to test protocol validations. */
  def testUnsupportedFeatures: Set[TableFeature] =
    if (isTesting) Set(TestUnsupportedReaderWriterFeature) else Set.empty

  private val allDependentFeaturesMap: Map[TableFeature, Set[TableFeature]] = {
    val dependentFeatureTuples =
      allSupportedFeaturesMap.values.toSeq.flatMap(f => f.requiredFeatures.map(_ -> f))
    dependentFeatureTuples
      .groupBy(_._1)
      .mapValues(_.map(_._2).toSet)
      .toMap
  }

  /** Get a [[TableFeature]] object by its name. */
  def featureNameToFeature(featureName: String): Option[TableFeature] =
    allSupportedFeaturesMap.get(featureName.toLowerCase(Locale.ROOT))

  /** Returns a set of [[TableFeature]]s that require the given feature to be enabled. */
  def getDependentFeatures(feature: TableFeature): Set[TableFeature] =
    allDependentFeaturesMap.getOrElse(feature, Set.empty)

  /**
   * Extracts the removed features by comparing new and old protocols.
   * Returns None if there are no removed features.
   */
  protected def getDroppedFeatures(
      newProtocol: Protocol,
      oldProtocol: Protocol): Set[TableFeature] = {
    val newFeatureNames = newProtocol.implicitlyAndExplicitlySupportedFeatures
    val oldFeatureNames = oldProtocol.implicitlyAndExplicitlySupportedFeatures
    oldFeatureNames -- newFeatureNames
  }

  /** Identifies whether there was any feature removal between two protocols. */
  def isProtocolRemovingFeatures(newProtocol: Protocol, oldProtocol: Protocol): Boolean = {
    getDroppedFeatures(newProtocol = newProtocol, oldProtocol = oldProtocol).nonEmpty
  }

  /**
   * Identifies whether there were any features with requiresHistoryProtection removed
   * between the two protocols.
   */
  def isProtocolRemovingFeatureWithHistoryProtection(
      newProtocol: Protocol,
      oldProtocol: Protocol): Boolean = {
    getDroppedFeatures(newProtocol = newProtocol, oldProtocol = oldProtocol).exists {
        case r: RemovableFeature if r.requiresHistoryProtection => true
        case _ => false
      }
  }

  /**
   * Validates whether all requirements of a removed feature hold against the provided snapshot.
   */
  def validateFeatureRemovalAtSnapshot(
      newProtocol: Protocol,
      oldProtocol: Protocol,
      snapshot: Snapshot): Boolean = {
    val droppedFeatures = TableFeature.getDroppedFeatures(
      newProtocol = newProtocol,
      oldProtocol = oldProtocol)
    val droppedFeature = droppedFeatures match {
      case f if f.size == 1 => f.head
      // We do not support dropping more than one feature at a time so we have to reject
      // the validation.
      case f if f.size > 1 => return false
      case _ => return true
    }

    droppedFeature match {
      case feature: RemovableFeature => feature.validateRemoval(snapshot)
      case _ => throw DeltaErrors.dropTableFeatureFeatureNotSupportedByClient(droppedFeature.name)
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
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    DeltaConfigs.IS_APPEND_ONLY.fromMetaData(metadata)
  }
}

object InvariantsTableFeature
  extends LegacyWriterFeature(name = "invariants", minWriterVersion = 2)
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    Invariants.getFromSchema(metadata.schema, spark).nonEmpty
  }
}

object CheckConstraintsTableFeature
  extends LegacyWriterFeature(name = "checkConstraints", minWriterVersion = 3)
  with FeatureAutomaticallyEnabledByMetadata
  with RemovableFeature {
  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    Constraints.getCheckConstraints(metadata, spark).nonEmpty
  }

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    CheckConstraintsPreDowngradeTableFeatureCommand(table)

  override def validateRemoval(snapshot: Snapshot): Boolean =
    Constraints.getCheckConstraintNames(snapshot.metadata).isEmpty

  override def actionUsesFeature(action: Action): Boolean = {
    // This method is never called, as it is only used for ReaderWriterFeatures.
    throw new UnsupportedOperationException()
  }
}

object ChangeDataFeedTableFeature
  extends LegacyWriterFeature(name = "changeDataFeed", minWriterVersion = 4)
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    DeltaConfigs.CHANGE_DATA_FEED.fromMetaData(metadata)
  }
}

object GeneratedColumnsTableFeature
  extends LegacyWriterFeature(name = "generatedColumns", minWriterVersion = 4)
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
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
  with RemovableFeature
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.columnMappingMode match {
      case NoMapping => false
      case _ => true
    }
  }

  override def validateRemoval(snapshot: Snapshot): Boolean = {
    val schemaHasNoColumnMappingMetadata =
      !DeltaColumnMapping.schemaHasColumnMappingMetadata(snapshot.schema)
    val metadataHasNoMappingMode = snapshot.metadata.columnMappingMode match {
      case NoMapping => true
      case _ => false
    }
    schemaHasNoColumnMappingMetadata && metadataHasNoMappingMode
  }

  override def actionUsesFeature(action: Action): Boolean = action match {
      case m: Metadata => DeltaConfigs.COLUMN_MAPPING_MODE.fromMetaData(m) != NoMapping
      case _ => false
    }

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    ColumnMappingPreDowngradeCommand(table)
}

object IdentityColumnsTableFeature
  extends LegacyWriterFeature(name = "identityColumns", minWriterVersion = 6)
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    ColumnWithDefaultExprUtils.hasIdentityColumn(metadata.schema)
  }
}

object TimestampNTZTableFeature extends ReaderWriterFeature(name = "timestampNtz")
    with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol, metadata: Metadata, spark: SparkSession): Boolean = {
    SchemaUtils.checkForTimestampNTZColumnsRecursively(metadata.schema)
  }
}

object RedirectReaderWriterFeature
  extends ReaderWriterFeature(name = "redirectReaderWriter-preview")
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
    protocol: Protocol,
    metadata: Metadata,
    spark: SparkSession
  ): Boolean = RedirectReaderWriter.isFeatureSet(metadata)

  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true
}

object RedirectWriterOnlyFeature extends WriterFeature(name = "redirectWriterOnly-preview")
  with FeatureAutomaticallyEnabledByMetadata {
  override def metadataRequiresFeatureToBeEnabled(
    protocol: Protocol,
    metadata: Metadata,
    spark: SparkSession
  ): Boolean = RedirectWriterOnly.isFeatureSet(metadata)

  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true
}

trait BinaryVariantTableFeature {
  def forcePreviewTableFeature: Boolean = SparkSession
    .getActiveSession
    .map(_.conf.get(DeltaSQLConf.FORCE_USE_PREVIEW_VARIANT_FEATURE))
    .getOrElse(false)
}

/**
 * Preview feature for variant. The preview feature isn't enabled automatically anymore when
 * variants are present in the table schema and the GA feature is used instead.
 *
 * Note: Users can manually add both the preview and stable features to a table using ADD FEATURE,
 * although that's undocumented. The feature spec did not change between preview and GA so the two
 * feature specifications are compatible and supported.
 */
object VariantTypePreviewTableFeature extends ReaderWriterFeature(name = "variantType-preview")
  with FeatureAutomaticallyEnabledByMetadata
    with BinaryVariantTableFeature {
  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol, metadata: Metadata, spark: SparkSession): Boolean = {
    if (forcePreviewTableFeature) {
      SchemaUtils.checkForVariantTypeColumnsRecursively(metadata.schema) &&
      // Do not require this table feature to be enabled when the 'variantType' table feature is
      // enabled so existing tables with variant columns with only 'variantType' and not
      // 'variantType-preview' can be operated on when the 'FORCE_USE_PREVIEW_VARIANT_FEATURE'
      // config is enabled.
      !protocol.isFeatureSupported(VariantTypeTableFeature)
    } else {
      false
    }
  }
}

object VariantTypeTableFeature extends ReaderWriterFeature(name = "variantType")
    with FeatureAutomaticallyEnabledByMetadata
    with BinaryVariantTableFeature {
  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol, metadata: Metadata, spark: SparkSession): Boolean = {
    if (forcePreviewTableFeature) {
      false
    } else {
      SchemaUtils.checkForVariantTypeColumnsRecursively(metadata.schema) &&
      // Do not require this table feature to be enabled when the 'variantType-preview' table
      // feature is enabled so old tables with only the preview table feature can be read.
      !protocol.isFeatureSupported(VariantTypePreviewTableFeature)
    }
  }
}

object DeletionVectorsTableFeature
  extends ReaderWriterFeature(name = "deletionVectors")
  with RemovableFeature
  with FeatureAutomaticallyEnabledByMetadata {
  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.fromMetaData(metadata)
  }

  /**
   * Validate whether all deletion vector traces are removed from the snapshot.
   *
   * Note, we do not need to validate whether DV tombstones exist. These are added in the
   * pre-downgrade stage and always cover all DVs within the retention period. This invariant can
   * never change unless we enable again DVs. If DVs are enabled before the protocol downgrade
   * we will abort the operation.
   */
  override def validateRemoval(snapshot: Snapshot): Boolean = {
    val dvsWritable = DeletionVectorUtils.deletionVectorsWritable(snapshot)
    val dvsExist = snapshot.numDeletionVectorsOpt.getOrElse(0L) > 0

    !(dvsWritable || dvsExist)
  }

  override def actionUsesFeature(action: Action): Boolean = {
    action match {
      case m: Metadata => DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.fromMetaData(m)
      case a: AddFile => a.deletionVector != null
      case r: RemoveFile => r.deletionVector != null
      // In general, CDC actions do not contain DVs. We added this for safety.
      case cdc: AddCDCFile => cdc.deletionVector != null
      case _ => false
    }
  }

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    DeletionVectorsPreDowngradeCommand(table)
}

object RowTrackingFeature extends WriterFeature(name = "rowTracking")
  with FeatureAutomaticallyEnabledByMetadata {
  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean =
    DeltaConfigs.ROW_TRACKING_ENABLED.fromMetaData(metadata)

  override def requiredFeatures: Set[TableFeature] = Set(DomainMetadataTableFeature)
}

object DomainMetadataTableFeature extends WriterFeature(name = "domainMetadata")

object IcebergCompatV1TableFeature extends WriterFeature(name = "icebergCompatV1")
  with FeatureAutomaticallyEnabledByMetadata {

  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = IcebergCompatV1.isEnabled(metadata)

  override def requiredFeatures: Set[TableFeature] = Set(ColumnMappingTableFeature)
}

object IcebergCompatV2TableFeature extends WriterFeature(name = "icebergCompatV2")
  with FeatureAutomaticallyEnabledByMetadata {

  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = IcebergCompatV2.isEnabled(metadata)

  override def requiredFeatures: Set[TableFeature] = Set(ColumnMappingTableFeature)
}

/**
 * Clustering table feature is enabled when a table is created with CLUSTER BY clause.
 */
object ClusteringTableFeature extends WriterFeature("clustering") {
  override val requiredFeatures: Set[TableFeature] = Set(DomainMetadataTableFeature)
}

/**
 * This table feature represents support for column DEFAULT values for Delta Lake. With this
 * feature, it is possible to assign default values to columns either at table creation time or
 * later by using commands of the form: ALTER TABLE t ALTER COLUMN c SET DEFAULT v. Thereafter,
 * queries from the table will return the specified default value instead of NULL when the
 * corresponding field is not present in storage.
 *
 * We create this as a writer-only feature rather than a reader/writer feature in order to simplify
 * the query execution implementation for scanning Delta tables. This means that commands of the
 * following form are not allowed: ALTER TABLE t ADD COLUMN c DEFAULT v. The reason is that when
 * commands of that form execute (such as for other data sources like CSV or JSON), then the data
 * source scan implementation must take responsibility to return the supplied default value for all
 * rows, including those previously present in the table before the command executed. We choose to
 * avoid this complexity for Delta table scans, so we make this a writer-only feature instead.
 * Therefore, the analyzer can take care of the entire job when processing commands that introduce
 * new rows into the table by injecting the column default value (if present) into the corresponding
 * query plan. This comes at the expense of preventing ourselves from easily adding a default value
 * to an existing non-empty table, because all data files would need to be rewritten to include the
 * new column value in an expensive backfill.
 */
object AllowColumnDefaultsTableFeature extends WriterFeature(name = "allowColumnDefaults")


/**
 * V2 Checkpoint table feature is for checkpoints with sidecars and the new format and
 * file naming scheme.
 */
object V2CheckpointTableFeature
  extends ReaderWriterFeature(name = "v2Checkpoint")
  with RemovableFeature
  with FeatureAutomaticallyEnabledByMetadata {

  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  private def isV2CheckpointSupportNeededByMetadata(metadata: Metadata): Boolean =
    DeltaConfigs.CHECKPOINT_POLICY.fromMetaData(metadata).needsV2CheckpointSupport

  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = isV2CheckpointSupportNeededByMetadata(metadata)

  override def validateRemoval(snapshot: Snapshot): Boolean = {
    // Fail validation if v2 checkpoints are still enabled in the current snapshot
    if (isV2CheckpointSupportNeededByMetadata(snapshot.metadata)) return false

    // Validation also fails if the current snapshot might depend on a v2 checkpoint.
    // NOTE: Empty and preloaded checkpoint providers never reference v2 checkpoints.
    snapshot.checkpointProvider match {
      case p if p.isEmpty => true
      case _: PreloadedCheckpointProvider => true
      case lazyProvider: LazyCompleteCheckpointProvider =>
        lazyProvider.underlyingCheckpointProvider.isInstanceOf[PreloadedCheckpointProvider]
      case _ => false
    }
  }

  override def actionUsesFeature(action: Action): Boolean = action match {
    case m: Metadata => isV2CheckpointSupportNeededByMetadata(m)
    case _: CheckpointMetadata => true
    case _: SidecarFile => true
    case _ => false
  }

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    V2CheckpointPreDowngradeCommand(table)
}

/** Table feature to represent tables whose commits are managed by separate commit-coordinator */
object CoordinatedCommitsTableFeature
  extends WriterFeature(name = "coordinatedCommits-preview")
    with FeatureAutomaticallyEnabledByMetadata
    with RemovableFeature {

  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.fromMetaData(metadata).nonEmpty
  }

  override def requiredFeatures: Set[TableFeature] =
    Set(InCommitTimestampTableFeature, VacuumProtocolCheckTableFeature)

  override def preDowngradeCommand(table: DeltaTableV2)
      : PreDowngradeTableFeatureCommand = CoordinatedCommitsPreDowngradeCommand(table)

  override def validateRemoval(snapshot: Snapshot): Boolean = {
    !CoordinatedCommitsUtils.tablePropertiesPresent(snapshot.metadata) &&
      !CoordinatedCommitsUtils.unbackfilledCommitsPresent(snapshot)
  }

  // This is a writer feature, so it should directly return false.
  override def actionUsesFeature(action: Action): Boolean = false
}

/** Common base shared by the preview and stable type widening table features. */
abstract class TypeWideningTableFeatureBase(name: String) extends ReaderWriterFeature(name)
    with RemovableFeature {

  protected def isTypeWideningSupportNeededByMetadata(metadata: Metadata): Boolean =
    DeltaConfigs.ENABLE_TYPE_WIDENING.fromMetaData(metadata)

  override def validateRemoval(snapshot: Snapshot): Boolean =
    !isTypeWideningSupportNeededByMetadata(snapshot.metadata) &&
      !TypeWideningMetadata.containsTypeWideningMetadata(snapshot.metadata.schema)

  override def actionUsesFeature(action: Action): Boolean =
    action match {
      case m: Metadata => TypeWideningMetadata.containsTypeWideningMetadata(m.schema)
      case _ => false
    }

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    TypeWideningPreDowngradeCommand(table)
}

/**
 * Feature used for the preview phase of type widening. Tables that enabled this feature during the
 * preview will keep being supported after the preview.
 */
object TypeWideningPreviewTableFeature
  extends TypeWideningTableFeatureBase(name = "typeWidening-preview")
  with FeatureAutomaticallyEnabledByMetadata {
  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = isTypeWideningSupportNeededByMetadata(metadata) &&
    // Don't automatically enable the preview feature if the stable feature is already supported.
    !protocol.isFeatureSupported(TypeWideningTableFeature)
}

/**
 * Stable feature for type widening. The stable feature isn't enabled automatically yet
 * when setting the type widening table property as the feature is still in preview in this version.
 * The feature spec is finalized though and by supporting the stable feature here we guarantee that
 * this version can already read any table created in the future.
 *
 * Note: Users can manually add both the preview and stable features to a table using ADD FEATURE,
 * although that's undocumented for type widening. This is allowed: the two feature specifications
 * are compatible and supported.
 */
object TypeWideningTableFeature
  extends TypeWideningTableFeatureBase(name = "typeWidening")

/**
 * inCommitTimestamp table feature is a writer feature that makes
 * every writer write a monotonically increasing timestamp inside the commit file.
 */
object InCommitTimestampTableFeature
  extends WriterFeature(name = "inCommitTimestamp")
  with FeatureAutomaticallyEnabledByMetadata
  with RemovableFeature {

  override def automaticallyUpdateProtocolOfExistingTables: Boolean = true

  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(metadata)
  }

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    InCommitTimestampsPreDowngradeCommand(table)


  /**
   * As per the spec, we can disable ICT by just setting
   * [[DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED]] to `false`. There is no need to remove the
   * provenance properties. However, [[InCommitTimestampsPreDowngradeCommand]] will try to remove
   * these properties because they can be removed as part of the same metadata update that sets
   * [[DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED]] to `false`. We check all three properties here
   * as well for consistency.
   */
  override def validateRemoval(snapshot: Snapshot): Boolean = {
    val provenancePropertiesAbsent = Seq(
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.key,
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.key)
      .forall(!snapshot.metadata.configuration.contains(_))
    val ictEnabledInMetadata =
      DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(snapshot.metadata)
    provenancePropertiesAbsent && !ictEnabledInMetadata
  }

  // Writer features should directly return false, as it is only used for reader+writer features.
  override def actionUsesFeature(action: Action): Boolean = false
}

/**
 * A ReaderWriter table feature for VACUUM. If this feature is enabled:
 * A writer should follow one of the following:
 *   1. Non-Support for Vacuum: Writers can explicitly state that they do not support VACUUM for
 *      any table, regardless of whether the Vacuum Protocol Check Table feature exists.
 *   2. Implement Writer Protocol Check: Ensure that the VACUUM implementation includes a writer
 *      protocol check before any file deletions occur.
 * Readers don't need to understand or change anything new; they just need to acknowledge the
 * feature exists
 */
object VacuumProtocolCheckTableFeature
  extends ReaderWriterFeature(name = "vacuumProtocolCheck")
  with RemovableFeature {

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand = {
    VacuumProtocolCheckPreDowngradeCommand(table)
  }

  // The delta snapshot doesn't have any trace of the [[VacuumProtocolCheckTableFeature]] feature.
  // Other than it being present in PROTOCOL, which will be handled by the table feature downgrade
  // command once this method returns true.
  override def validateRemoval(snapshot: Snapshot): Boolean = true

  // None of the actions uses [[VacuumProtocolCheckTableFeature]]
  override def actionUsesFeature(action: Action): Boolean = false
}

/**
 * Writer feature that enforces writers to cleanup metadata iff metadata can be cleaned up to
 * requireCheckpointProtectionBeforeVersion in one go. This means that a single cleanup
 * operation should truncate up to requireCheckpointProtectionBeforeVersion as opposed to
 * several cleanup operations truncating in chunks.
 *
 * The are two exceptions to this rule. If any of the two holds, the rule
 * above can be ignored:
 *
 *   a) The writer verifies it supports all protocols between
 *      [start, min(requireCheckpointProtectionBeforeVersion, targetCleanupVersion)] versions
 *      it intends to truncate.
 *   b) The writer does not create any checkpoints during history cleanup and does not erase any
 *      checkpoints after the truncation version.
 *
 * The CheckpointProtectionTableFeature can only be removed if history is truncated up to
 * at least requireCheckpointProtectionBeforeVersion.
 */
object CheckpointProtectionTableFeature
    extends WriterFeature(name = "checkpointProtection")
    with RemovableFeature {
  def getCheckpointProtectionVersion(snapshot: Snapshot): Long = {
    if (!snapshot.protocol.isFeatureSupported(CheckpointProtectionTableFeature)) return 0
    DeltaConfigs.REQUIRE_CHECKPOINT_PROTECTION_BEFORE_VERSION.fromMetaData(snapshot.metadata)
  }

  def metadataWithCheckpointProtection(metadata: Metadata, version: Long): Metadata = {
    val versionPropKey = DeltaConfigs.REQUIRE_CHECKPOINT_PROTECTION_BEFORE_VERSION.key
    val versionConf = versionPropKey -> version.toString
    metadata.copy(configuration = metadata.configuration + versionConf)
  }

  /** Verify whether any deltas exist between version 0 to toVersion (inclusive). */
  private def deltasUpToVersionAreTruncated(deltaLog: DeltaLog, toVersion: Long): Boolean = {
    deltaLog
      .getChangeLogFiles(startVersion = 0, endVersion = toVersion, failOnDataLoss = false)
      .map { case (_, file) => file }
      .filter(FileNames.isDeltaFile)
      .take(1).isEmpty
  }

  def historyPriorToCheckpointProtectionVersionIsTruncated(snapshot: Snapshot): Boolean = {
    val checkpointProtectionVersion = getCheckpointProtectionVersion(snapshot)
    if (checkpointProtectionVersion <= 0) return true

    val deltaLog = snapshot.deltaLog
    // In most cases, the earliest checkpoint matches the version of the earliest commit. This is
    // not true for new tables that were never cleaned up. Furthermore, if there is no checkpoint it
    // means history is not truncated.
    deltaLog.findEarliestReliableCheckpoint.exists(_ >= checkpointProtectionVersion) &&
      deltasUpToVersionAreTruncated(deltaLog, checkpointProtectionVersion - 1)
  }

  /**
   * This is a special feature in the sense that it requires history truncation but implements it
   * as part of its downgrade process. This is implemented like this for 2 reasons:
   *
   *  1. It allows us to remove the feature table property after the clean up in the preDowngrade
   *     command is successful.
   *  2. It does not require to scan the history for features traces as long as all history
   *     before requireCheckpointProtectionBeforeVersion is truncated.
   */
  override def requiresHistoryProtection: Boolean = false

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand = {
    CheckpointProtectionPreDowngradeCommand(table)
  }

  /** Returns true if table property is absent. */
  override def validateRemoval(snapshot: Snapshot): Boolean = {
    val property = DeltaConfigs.REQUIRE_CHECKPOINT_PROTECTION_BEFORE_VERSION.key
    !snapshot.metadata.configuration.contains(property)
  }

  /**
   * The feature uses the `requireCheckpointProtectionBeforeVersion` property. This is removed when
   * dropping the feature but we allow it to exist in the history. This is to allow history
   * truncation at the boundary of requireCheckpointProtectionBeforeVersion rather than the last
   * 24 hours. Otherwise, dropping the feature would always require 24 hour waiting time.
   */
  override def actionUsesFeature(action: Action): Boolean = false
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
      protocol: Protocol,
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
      protocol: Protocol,
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
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }
}

object TestRemovableWriterFeature
  extends WriterFeature(name = "testRemovableWriter")
  with FeatureAutomaticallyEnabledByMetadata
  with RemovableFeature {

  val TABLE_PROP_KEY = "_123TestRemovableWriter321_"
  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }

  /** Make sure the property is not enabled on the table. */
  override def validateRemoval(snapshot: Snapshot): Boolean =
    !snapshot.metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    TestWriterFeaturePreDowngradeCommand(table)

  override def actionUsesFeature(action: Action): Boolean = false
}

/** Test feature that appears unsupported and it is used for testing protocol checks. */
object TestUnsupportedReaderWriterFeature
    extends ReaderWriterFeature(name = "testUnsupportedReaderWriter")
    with RemovableFeature {

  override def validateRemoval(snapshot: Snapshot): Boolean = true

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    TestUnsupportedReaderWriterFeaturePreDowngradeCommand(table)

  override def actionUsesFeature(action: Action): Boolean = false
}

private[sql] object TestRemovableWriterFeatureWithDependency
  extends WriterFeature(name = "testRemovableWriterFeatureWithDependency")
  with FeatureAutomaticallyEnabledByMetadata
  with RemovableFeature {

  val TABLE_PROP_KEY = "_123TestRemovableWriterFeatureWithDependency321_"
  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }

  /** Make sure the property is not enabled on the table. */
  override def validateRemoval(snapshot: Snapshot): Boolean =
    !snapshot.metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    TestWriterFeaturePreDowngradeCommand(table)

  override def actionUsesFeature(action: Action): Boolean = false

  override def requiredFeatures: Set[TableFeature] =
    Set(TestRemovableReaderWriterFeature, TestRemovableWriterFeature)
}

object TestRemovableReaderWriterFeature
  extends ReaderWriterFeature(name = "testRemovableReaderWriter")
    with FeatureAutomaticallyEnabledByMetadata
    with RemovableFeature {

  val TABLE_PROP_KEY = "_123TestRemovableReaderWriter321_"
  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }

  /** Make sure the property is not enabled on the table. */
  override def validateRemoval(snapshot: Snapshot): Boolean =
    !snapshot.metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)

  override def actionUsesFeature(action: Action): Boolean = action match {
    case m: Metadata => m.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
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
      protocol: Protocol,
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
      protocol: Protocol,
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
      protocol: Protocol, metadata: Metadata, spark: SparkSession): Boolean = {
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

object TestRemovableWriterWithHistoryTruncationFeature
  extends WriterFeature(name = "TestRemovableWriterWithHistoryTruncationFeature")
  with FeatureAutomaticallyEnabledByMetadata
  with RemovableFeature {

  val TABLE_PROP_KEY = "_123TestRemovableWriterWithHistoryTruncationFeature321_"

  override def metadataRequiresFeatureToBeEnabled(
      protocol: Protocol,
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
  }

  /** Make sure the property is not enabled on the table. */
  override def validateRemoval(snapshot: Snapshot): Boolean =
    !snapshot.metadata.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)

  override def preDowngradeCommand(table: DeltaTableV2): PreDowngradeTableFeatureCommand =
    TestWriterWithHistoryValidationFeaturePreDowngradeCommand(table)

  override def actionUsesFeature(action: Action): Boolean = action match {
    case m: Metadata => m.configuration.get(TABLE_PROP_KEY).exists(_.toBoolean)
    case _ => false
  }

  override def requiresHistoryProtection: Boolean = true
}
