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
import org.apache.spark.sql.delta.constraints.{Constraints, Invariants}
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}

import org.apache.spark.sql.SparkSession
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
      DeletionVectorsTableFeature)
    if (DeltaUtils.isTesting) {
      features ++= Set(
        TestLegacyWriterFeature,
        TestLegacyReaderWriterFeature,
        TestWriterFeature,
        TestWriterMetadataNoAutoUpdateFeature,
        TestReaderWriterFeature,
        TestReaderWriterMetadataAutoUpdateFeature,
        TestReaderWriterMetadataNoAutoUpdateFeature,
        TestFeatureWithDependency,
        TestFeatureWithTransitiveDependency,
        TestWriterFeatureWithTransitiveDependency,
        // Row IDs are still under development and only available in testing.
        RowTrackingFeature)
    }
    val featureMap = features.map(f => f.name.toLowerCase(Locale.ROOT) -> f).toMap
    require(features.size == featureMap.size, "Lowercase feature names must not duplicate.")
    featureMap
  }

  /** Get a [[TableFeature]] object by its name. */
  def featureNameToFeature(featureName: String): Option[TableFeature] =
    allSupportedFeaturesMap.get(featureName.toLowerCase(Locale.ROOT))
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
