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

package org.apache.spark.sql.delta.actions

// scalastyle:off import.ordering.noEmptyLine
import java.net.URI
import java.sql.Timestamp
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{JsonUtils, Utils => DeltaUtils}
import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils

object Action {
  /**
   * The maximum version of the protocol that this version of Delta understands by default.
   *
   * Use [[supportedProtocolVersion()]] instead, except to define new feature-gated versions.
   */
  private[actions] val readerVersion = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_READER_VERSION
  private[actions] val writerVersion = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION
  private[actions] val protocolVersion: Protocol = Protocol(readerVersion, writerVersion)

  /**
   * The maximum protocol version we are currently allowed to use, with or without all recognized
   * features.
   */
  private[delta] def supportedProtocolVersion(withAllFeatures: Boolean = true): Protocol = {
    if (withAllFeatures) {
      protocolVersion.withFeatures(TableFeature.allSupportedFeaturesMap.values)
    } else {
      protocolVersion
    }
  }

  /** All reader protocol version numbers supported by the system. */
  private[delta] lazy val supportedReaderVersionNumbers: Set[Int] = {
    val allVersions =
      supportedProtocolVersion().implicitlyAndExplicitlySupportedFeatures.map(_.minReaderVersion) +
      1 // Version 1 does not introduce new feature, it's always supported.
    if (DeltaUtils.isTesting) {
      allVersions + 0 // Allow Version 0 in tests
    } else {
      allVersions - 0 // Delete 0 produced by writer-only features
    }
  }

  /** All writer protocol version numbers supported by the system. */
  private[delta] lazy val supportedWriterVersionNumbers: Set[Int] = {
    val allVersions =
      supportedProtocolVersion().implicitlyAndExplicitlySupportedFeatures.map(_.minWriterVersion) +
        1 // Version 1 does not introduce new feature, it's always supported.
    if (DeltaUtils.isTesting) {
      allVersions + 0 // Allow Version 0 in tests
    } else {
      allVersions - 0 // Delete 0 produced by reader-only features - we don't have any - for safety
    }
  }

  def fromJson(json: String): Action = {
    JsonUtils.mapper.readValue[SingleAction](json).unwrap
  }

  lazy val logSchema = ExpressionEncoder[SingleAction].schema
}

/**
 * Represents a single change to the state of a Delta table. An order sequence
 * of actions can be replayed using [[InMemoryLogReplay]] to derive the state
 * of the table at a given point in time.
 */
sealed trait Action {
  def wrap: SingleAction
  def json: String = JsonUtils.toJson(wrap)
}

/**
 * Used to block older clients from reading or writing the log when backwards incompatible changes
 * are made to the protocol. Readers and writers are responsible for checking that they meet the
 * minimum versions before performing any other operations.
 *
 * This action allows us to explicitly block older clients in the case of a breaking change to the
 * protocol. Absent a protocol change, Clients MUST silently ignore messages and fields that they
 * do not understand.
 *
 * Note: Please initialize this class using the companion object's `apply` method, which will
 * assign correct values (`Set()` vs `None`) to [[readerFeatures]] and [[writerFeatures]].
 */
case class Protocol private (
    minReaderVersion: Int,
    minWriterVersion: Int,
    @JsonInclude(Include.NON_ABSENT) // write to JSON only when the field is not `None`
    readerFeatures: Option[Set[String]],
    @JsonInclude(Include.NON_ABSENT)
    writerFeatures: Option[Set[String]])
  extends Action
  with TableFeatureSupport {
  // Correctness check
  // Reader and writer versions must match the status of reader and writer features
  require(
    supportsReaderFeatures == readerFeatures.isDefined,
    "Mismatched minReaderVersion and readerFeatures.")
  require(
    supportsWriterFeatures == writerFeatures.isDefined,
    "Mismatched minWriterVersion and writerFeatures.")

  // When reader is on table features, writer must be on table features too
  if (supportsReaderFeatures && !supportsWriterFeatures) {
    throw DeltaErrors.tableFeatureReadRequiresWriteException(
      TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)
  }

  override def wrap: SingleAction = SingleAction(protocol = this)

  /**
   * Return a reader-friendly string representation of this Protocol.
   *
   * Returns the protocol versions and referenced features when the protocol does support table
   * features, such as `3,7,{},{appendOnly}` and `2,7,None,{appendOnly}`. Otherwise returns only
   * the protocol version such as `2,6`.
   */
  @JsonIgnore
  lazy val simpleString: String = {
    if (!supportsReaderFeatures && !supportsWriterFeatures) {
      s"$minReaderVersion,$minWriterVersion"
    } else {
      val readerFeaturesStr = readerFeatures
        .map(_.toSeq.sorted.mkString("[", ",", "]"))
        .getOrElse("None")
      val writerFeaturesStr = writerFeatures
        .map(_.toSeq.sorted.mkString("[", ",", "]"))
        .getOrElse("None")
      s"$minReaderVersion,$minWriterVersion,$readerFeaturesStr,$writerFeaturesStr"
    }
  }

  override def toString: String = s"Protocol($simpleString)"
}

object Protocol {
  import TableFeatureProtocolUtils._

  val MIN_READER_VERSION_PROP = "delta.minReaderVersion"
  val MIN_WRITER_VERSION_PROP = "delta.minWriterVersion"

  /**
   * Construct a [[Protocol]] case class of the given reader and writer versions. This method will
   * initialize table features fields when reader and writer versions are capable.
   */
  def apply(
      minReaderVersion: Int = Action.readerVersion,
      minWriterVersion: Int = Action.writerVersion): Protocol = {
    new Protocol(
      minReaderVersion = minReaderVersion,
      minWriterVersion = minWriterVersion,
      readerFeatures = if (supportsReaderFeatures(minReaderVersion)) Some(Set()) else None,
      writerFeatures = if (supportsWriterFeatures(minWriterVersion)) Some(Set()) else None)
  }

  /**
   * Picks the protocol version for a new table given the Delta table metadata. The result
   * satisfies all active features in the metadata and protocol-related configs in table
   * properties, i.e., configs with keys [[MIN_READER_VERSION_PROP]], [[MIN_WRITER_VERSION_PROP]],
   * and [[FEATURE_PROP_PREFIX]]. This method will also consider protocol-related configs: default
   * reader version, default writer version, and features enabled by
   * [[DEFAULT_FEATURE_PROP_PREFIX]].
   */
  def forNewTable(spark: SparkSession, metadataOpt: Option[Metadata]): Protocol = {
    // `minProtocolComponentsFromMetadata` does not consider sessions defaults,
    // so we must copy sessions defaults to table metadata.
    val conf = spark.sessionState.conf
    val ignoreProtocolDefaults = DeltaConfigs.ignoreProtocolDefaultsIsSet(
      sqlConfs = conf,
      tableConf = metadataOpt.map(_.configuration).getOrElse(Map.empty))
    val defaultGlobalConf = if (ignoreProtocolDefaults) {
      Map(MIN_READER_VERSION_PROP -> 1.toString, MIN_WRITER_VERSION_PROP -> 1.toString)
    } else {
      Map(
        MIN_READER_VERSION_PROP ->
          conf.getConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION).toString,
        MIN_WRITER_VERSION_PROP ->
          conf.getConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION).toString)
    }
    val overrideGlobalConf = DeltaConfigs
      .mergeGlobalConfigs(
        sqlConfs = spark.sessionState.conf,
        tableConf = Map.empty,
        ignoreProtocolConfsOpt = Some(ignoreProtocolDefaults))
      // We care only about protocol related stuff
      .filter { case (k, _) => TableFeatureProtocolUtils.isTableProtocolProperty(k) }
    var metadata = metadataOpt.getOrElse(Metadata())
    // Priority: user-provided > override of session defaults > session defaults
    metadata = metadata.copy(configuration =
      defaultGlobalConf ++ overrideGlobalConf ++ metadata.configuration)

    val (readerVersion, writerVersion, enabledFeatures) =
      minProtocolComponentsFromMetadata(spark, metadata)
    Protocol(readerVersion, writerVersion).withFeatures(enabledFeatures)
  }

  /**
   * Extracts all table features that are enabled by the given metadata.
   */
  def extractAutomaticallyEnabledFeatures(
      spark: SparkSession, metadata: Metadata): Set[TableFeature] = {
    val enabledFeatures = mutable.Set[TableFeature]()
    TableFeature.allSupportedFeaturesMap.values.foreach {
      case feature: TableFeature with FeatureAutomaticallyEnabledByMetadata =>
        if (feature.metadataRequiresFeatureToBeEnabled(metadata, spark)) {
          enabledFeatures += feature
        }
      case _ => () // Do nothing. We only care about features that can be activated in metadata.
    }
    enabledFeatures.toSet
  }

  /**
   * Given the Delta table metadata, returns the minimum required reader and writer version that
   * satisfies all enabled features in the metadata and protocol-related configs in table
   * properties, i.e., configs with keys [[MIN_READER_VERSION_PROP]], [[MIN_WRITER_VERSION_PROP]],
   * and [[FEATURE_PROP_PREFIX]].
   *
   * This function returns the protocol versions and features individually instead of a
   * [[Protocol]], so the caller can identify the features that caused the protocol version. For
   * example, if the return values are (2, 5, columnMapping), the caller can safely ignore all
   * other features required by the protocol with a reader and writer version of 2 and 5.
   *
   * Note that this method does not consider protocol versions and features configured in session
   * defaults. To make them effective, copy them to `metadata` using
   * [[DeltaConfigs.mergeGlobalConfigs]].
   */
  def minProtocolComponentsFromMetadata(
      spark: SparkSession,
      metadata: Metadata): (Int, Int, Set[TableFeature]) = {
    val tableConf = metadata.configuration
    // There might be features enabled by the table properties aka
    // `CREATE TABLE ... TBLPROPERTIES ...`.
    val tablePropEnabledFeatures = getSupportedFeaturesFromTableConfigs(tableConf)
    val metaEnabledFeatures = extractAutomaticallyEnabledFeatures(spark, metadata)
    val allEnabledFeatures = tablePropEnabledFeatures ++ metaEnabledFeatures

    // Determine the min reader and writer version required by features in table properties or
    // metadata.
    // If any table property is specified:
    //   we start from (3, 7) or (0, 7) depending on the existence of any writer-only feature.
    // If there's no table property:
    //   if no feature is enabled or all features are legacy, we start from (0, 0);
    //   if any feature is native and is reader-writer, we start from (3, 7);
    //   otherwise we start from (0, 7) because there must exist a native writer-only feature.
    var (readerVersionFromFeatures, writerVersionFromFeatures) = {
      if (tablePropEnabledFeatures.exists(_.isReaderWriterFeature)) {
        (TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
      } else if (tablePropEnabledFeatures.nonEmpty) {
        (0, TABLE_FEATURES_MIN_WRITER_VERSION)
      } else if (metaEnabledFeatures.forall(_.isLegacyFeature)) { // also true for empty set
        (0, 0)
      } else if (metaEnabledFeatures.exists(f => !f.isLegacyFeature && f.isReaderWriterFeature)) {
        (TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
      } else {
        (0, TABLE_FEATURES_MIN_WRITER_VERSION)
      }
    }
    allEnabledFeatures.foreach { feature =>
      readerVersionFromFeatures = math.max(readerVersionFromFeatures, feature.minReaderVersion)
      writerVersionFromFeatures = math.max(writerVersionFromFeatures, feature.minWriterVersion)
    }

    // Protocol version provided in table properties can upgrade the protocol, but only when they
    // are higher than which required by the enabled features.
    val (readerVersionFromTableConfOpt, writerVersionFromTableConfOpt) =
      getProtocolVersionsFromTableConf(tableConf)

    // Decide the final protocol version:
    //   a. 1, aka the lowest version possible
    //   b. version required by manually enabled features and metadata features
    //   c. version defined as table properties
    val finalReaderVersion =
      Seq(1, readerVersionFromFeatures, readerVersionFromTableConfOpt.getOrElse(0)).max
    val finalWriterVersion =
      Seq(1, writerVersionFromFeatures, writerVersionFromTableConfOpt.getOrElse(0)).max

    (finalReaderVersion, finalWriterVersion, allEnabledFeatures)
  }

  /**
   * Given the Delta table metadata, returns the minimum required reader and writer version
   * that satisfies all enabled table features in the metadata plus all enabled features as a set.
   *
   * This function returns the protocol versions and features individually instead of a
   * [[Protocol]], so the caller can identify the features that caused the protocol version. For
   * example, if the return values are (2, 5, columnMapping), the caller can safely ignore all
   * other features required by the protocol with a reader and writer version of 2 and 5.
   *
   * This method does not process protocol-related configs in table properties or session
   * defaults, i.e., configs with keys [[MIN_READER_VERSION_PROP]], [[MIN_WRITER_VERSION_PROP]],
   * and [[FEATURE_PROP_PREFIX]].
   */
  def minProtocolComponentsFromAutomaticallyEnabledFeatures(
      spark: SparkSession,
      metadata: Metadata): (Int, Int, Set[TableFeature]) = {
    val enabledFeatures = extractAutomaticallyEnabledFeatures(spark, metadata)
    var (readerVersion, writerVersion) = (0, 0)
    enabledFeatures.foreach { feature =>
      readerVersion = math.max(readerVersion, feature.minReaderVersion)
      writerVersion = math.max(writerVersion, feature.minWriterVersion)
    }

    (readerVersion, writerVersion, enabledFeatures)
  }

  /** Cast the table property for the protocol version to an integer. */
  private def tryCastProtocolVersionToInt(key: String, value: String): Int = {
    try value.toInt
    catch {
      case _: NumberFormatException =>
        throw DeltaErrors.protocolPropNotIntException(key, value)
    }
  }

  def getReaderVersionFromTableConf(conf: Map[String, String]): Option[Int] = {
    conf.get(MIN_READER_VERSION_PROP).map(tryCastProtocolVersionToInt(MIN_READER_VERSION_PROP, _))
  }

  def getWriterVersionFromTableConf(conf: Map[String, String]): Option[Int] = {
    conf.get(MIN_WRITER_VERSION_PROP).map(tryCastProtocolVersionToInt(MIN_WRITER_VERSION_PROP, _))
  }

  def getProtocolVersionsFromTableConf(conf: Map[String, String]): (Option[Int], Option[Int]) = {
    (getReaderVersionFromTableConf(conf), getWriterVersionFromTableConf(conf))
  }

  /** Assert a table metadata contains no protocol-related table properties. */
  private def assertMetadataContainsNoProtocolProps(metadata: Metadata): Unit = {
    assert(
      !metadata.configuration.contains(MIN_READER_VERSION_PROP),
      "Should not have the " +
        s"protocol version ($MIN_READER_VERSION_PROP) as part of table properties")
    assert(
      !metadata.configuration.contains(MIN_WRITER_VERSION_PROP),
      "Should not have the " +
        s"protocol version ($MIN_WRITER_VERSION_PROP) as part of table properties")
    assert(
      !metadata.configuration.keys.exists(_.startsWith(FEATURE_PROP_PREFIX)),
      "Should not have " +
        s"table features (starts with '$FEATURE_PROP_PREFIX') as part of table properties")
    assert(
      !metadata.configuration.contains(DeltaConfigs.CREATE_TABLE_IGNORE_PROTOCOL_DEFAULTS.key),
      "Should not have the table property " +
        s"${DeltaConfigs.CREATE_TABLE_IGNORE_PROTOCOL_DEFAULTS.key} stored in table metadata")
  }

  /**
   * Upgrade the current protocol to satisfy all auto-update capable features required by the table
   * metadata. An Delta error will be thrown if a non-auto-update capable feature is required by
   * the metadata and not in the resulting protocol, in such a case the user must run `ALTER TABLE`
   * to add support for this feature beforehand using the `delta.feature.featureName` table
   * property.
   *
   * Refer to [[FeatureAutomaticallyEnabledByMetadata.automaticallyUpdateProtocolOfExistingTables]]
   * to know more about "auto-update capable" features.
   *
   * Note: this method only considers metadata-enabled features. To avoid confusion, the caller
   * must apply and remove protocol-related table properties from the metadata before calling this
   * method.
   */
  def upgradeProtocolFromMetadataForExistingTable(
      spark: SparkSession,
      metadata: Metadata,
      current: Protocol): Option[Protocol] = {
    assertMetadataContainsNoProtocolProps(metadata)

    val (readerVersion, writerVersion, minRequiredFeatures) =
      minProtocolComponentsFromAutomaticallyEnabledFeatures(spark, metadata)

    // Increment the reader and writer version to accurately add enabled legacy table features
    // either to the implicitly enabled table features or the table feature lists
    val required = Protocol(
      readerVersion.max(current.minReaderVersion), writerVersion.max(current.minWriterVersion))
      .withFeatures(minRequiredFeatures)
    if (!required.canUpgradeTo(current)) {
      // When the current protocol does not satisfy metadata requirement, some additional features
      // must be supported by the protocol. We assert those features can actually perform the
      // auto-update.
      assertMetadataTableFeaturesAutomaticallySupported(
        current.implicitlyAndExplicitlySupportedFeatures,
        required.implicitlyAndExplicitlySupportedFeatures)
      Some(required.merge(current))
    } else {
      None
    }
  }

  /**
   * Ensure all features listed in `currentFeatures` are also listed in `requiredFeatures`, or, if
   * one is not listed, it must be capable to auto-update a protocol.
   *
   * Refer to [[FeatureAutomaticallyEnabledByMetadata.automaticallyUpdateProtocolOfExistingTables]]
   * to know more about "auto-update capable" features.
   *
   * Note: Caller must make sure `requiredFeatures` is obtained from a min protocol that satisfies
   * a table metadata.
   */
  private def assertMetadataTableFeaturesAutomaticallySupported(
      currentFeatures: Set[TableFeature],
      requiredFeatures: Set[TableFeature]): Unit = {
    val (autoUpdateCapableFeatures, nonAutoUpdateCapableFeatures) =
      requiredFeatures.diff(currentFeatures)
        .collect { case f: FeatureAutomaticallyEnabledByMetadata => f }
        .partition(_.automaticallyUpdateProtocolOfExistingTables)
    if (nonAutoUpdateCapableFeatures.nonEmpty) {
      // The "current features" we give the user are which from the original protocol, plus
      // features newly supported by table properties in the current transaction, plus
      // metadata-enabled features that are auto-update capable. The first two are provided by
      // `currentFeatures`.
      throw DeltaErrors.tableFeaturesRequireManualEnablementException(
        nonAutoUpdateCapableFeatures,
        currentFeatures ++ autoUpdateCapableFeatures)
    }
  }

  /**
   * Verify that the table properties satisfy legality constraints. Throw an exception if not.
   */
  def assertTablePropertyConstraintsSatisfied(
      spark: SparkSession,
      metadata: Metadata,
      snapshot: Snapshot): Unit = {
    import DeltaTablePropertyValidationFailedSubClass._

    val tableName = if (metadata.name != null) metadata.name else metadata.id

    val configs = metadata.configuration.map { case (k, v) => k.toLowerCase(Locale.ROOT) -> v }
    val dvsEnabled = {
      val lowerCaseKey = DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key.toLowerCase(Locale.ROOT)
      configs.get(lowerCaseKey).exists(_.toBoolean)
    }
    if (dvsEnabled && metadata.format.provider != "parquet") {
      // DVs only work with parquet-based delta tables.
      throw new DeltaTablePropertyValidationFailedException(
        table = tableName,
        subClass = PersistentDeletionVectorsInNonParquetTable)
    }
    val manifestGenerationEnabled = {
      val lowerCaseKey = DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED.key.toLowerCase(Locale.ROOT)
      configs.get(lowerCaseKey).exists(_.toBoolean)
    }
    if (dvsEnabled && manifestGenerationEnabled) {
      throw new DeltaTablePropertyValidationFailedException(
        table = tableName,
        subClass = PersistentDeletionVectorsWithIncrementalManifestGeneration)
    }
    if (manifestGenerationEnabled) {
      // Only allow enabling this, if there are no DVs present.
      if (!DeletionVectorUtils.isTableDVFree(spark, snapshot)) {
        throw new DeltaTablePropertyValidationFailedException(
          table = tableName,
          subClass = ExistingDeletionVectorsWithIncrementalManifestGeneration)
      }
    }
  }
}

/**
 * Sets the committed version for a given application. Used to make operations
 * like streaming append idempotent.
 */
case class SetTransaction(
    appId: String,
    version: Long,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    lastUpdated: Option[Long]) extends Action {
  override def wrap: SingleAction = SingleAction(txn = this)
}

/** Actions pertaining to the addition and removal of files. */
sealed trait FileAction extends Action {
  val path: String
  val dataChange: Boolean
  @JsonIgnore
  val tags: Map[String, String]
  @JsonIgnore
  lazy val pathAsUri: URI = new URI(path)
  @JsonIgnore
  def numLogicalRecords: Option[Long]
  @JsonIgnore
  val partitionValues: Map[String, String]
  @JsonIgnore
  def getFileSize: Long

  /**
   * Return tag value if tags is not null and the tag present.
   */
  @JsonIgnore
  def getTag(tagName: String): Option[String] = Option(tags).flatMap(_.get(tagName))


  def toPath: Path = new Path(pathAsUri)
}

/**
 * Adds a new file to the table. When multiple [[AddFile]] file actions
 * are seen with the same `path` only the metadata from the last one is
 * kept.
 *
 * [[path]] is URL-encoded.
 */
case class AddFile(
    override val path: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    modificationTime: Long,
    override val dataChange: Boolean,
    stats: String = null,
    override val tags: Map[String, String] = null,
    deletionVector: DeletionVectorDescriptor = null
) extends FileAction {
  require(path.nonEmpty)

  override def wrap: SingleAction = SingleAction(add = this)

  def remove: RemoveFile = removeWithTimestamp()

  def removeWithTimestamp(
      timestamp: Long = System.currentTimeMillis(),
      dataChange: Boolean = true
    ): RemoveFile = {
    var newTags = tags
    // scalastyle:off
    val removedFile = RemoveFile(
      path, Some(timestamp), dataChange,
      extendedFileMetadata = Some(true), partitionValues, Some(size), newTags,
      deletionVector = deletionVector
    )
    removedFile.numLogicalRecords = numLogicalRecords
    removedFile.estLogicalFileSize = estLogicalFileSize
    // scalastyle:on
    removedFile
  }

  /**
   * Logically remove rows by associating a `deletionVector` with the file.
   * @param deletionVector: The descriptor of the DV that marks rows as deleted.
   * @param dataChange: When false, the actions are marked as no-data-change actions.
   */
  def removeRows(
        deletionVector: DeletionVectorDescriptor,
        dataChange: Boolean = true): (AddFile, RemoveFile) = {
    // Verify DV does not contain any invalid row indexes. Note, maxRowIndex is optional
    // and not all commands may set it when updating DVs.
    (numPhysicalRecords, deletionVector.maxRowIndex) match {
      case (Some(numPhysicalRecords), Some(maxRowIndex))
        if (maxRowIndex + 1 > numPhysicalRecords) =>
          throw DeltaErrors.deletionVectorInvalidRowIndex()
      case _ => // Nothing to check.
    }
    // We make sure maxRowIndex is not stored in the log.
    val dvDescriptorWithoutMaxRowIndex = deletionVector.maxRowIndex match {
      case Some(_) => deletionVector.copy(maxRowIndex = None)
      case _ => deletionVector
    }
    val withUpdatedDV =
      this.copy(deletionVector = dvDescriptorWithoutMaxRowIndex, dataChange = dataChange)
    val addFile = withUpdatedDV
    val removeFile = this.removeWithTimestamp(dataChange = dataChange)
    (addFile, removeFile)
  }

  /**
   * Return the unique id of the deletion vector, if present, or `None` if there's no DV.
   *
   * The unique id differentiates DVs, even if there are multiple in the same file
   * or the DV is stored inline.
   */
  @JsonIgnore
  def getDeletionVectorUniqueId: Option[String] = Option(deletionVector).map(_.uniqueId)


  @JsonIgnore
  lazy val insertionTime: Long = tag(AddFile.Tags.INSERTION_TIME).map(_.toLong)
    // From modification time in milliseconds to microseconds.
    .getOrElse(TimeUnit.MICROSECONDS.convert(modificationTime, TimeUnit.MILLISECONDS))


  def tag(tag: AddFile.Tags.KeyType): Option[String] = getTag(tag.name)

  def copyWithTag(tag: AddFile.Tags.KeyType, value: String): AddFile =
    copy(tags = Option(tags).getOrElse(Map.empty) + (tag.name -> value))

  def copyWithoutTag(tag: AddFile.Tags.KeyType): AddFile = {
    if (tags == null) {
      this
    } else {
      copy(tags = tags - tag.name)
    }
  }

  @JsonIgnore
  override def getFileSize: Long = size

  private case class ParsedStatsFields(
      numLogicalRecords: Option[Long],
      tightBounds: Option[Boolean])

  /**
   * Before serializing make sure deletionVector.maxRowIndex is not defined.
   * This is only a transient property and it is not intended to be stored in the log.
   */
  override def json: String = {
    if (deletionVector != null) assert(!deletionVector.maxRowIndex.isDefined)
    super.json
  }

  @JsonIgnore
  @transient
  private lazy val parsedStatsFields: Option[ParsedStatsFields] = {
    if (stats == null || stats.isEmpty) {
      None
    } else {
      val node = new ObjectMapper().readTree(stats)

      val numLogicalRecords = if (node.has("numRecords")) {
        Some(node.get("numRecords")).filterNot(_.isNull).map(_.asLong())
          .map(_ - numDeletedRecords)
      } else None
      val tightBounds = if (node.has("tightBounds")) {
        Some(node.get("tightBounds")).filterNot(_.isNull).map(_.asBoolean())
      } else None

      Some(ParsedStatsFields(numLogicalRecords, tightBounds))
    }
  }

  @JsonIgnore
  @transient
  override lazy val numLogicalRecords: Option[Long] =
    parsedStatsFields.flatMap(_.numLogicalRecords)

  /** Returns the number of records marked as deleted. */
  @JsonIgnore
  def numDeletedRecords: Long = if (deletionVector != null) deletionVector.cardinality else 0L

  /** Returns the total number of records, including those marked as deleted. */
  @JsonIgnore
  def numPhysicalRecords: Option[Long] = numLogicalRecords.map(_ + numDeletedRecords)

  /** Returns the approx size of the remaining records after excluding the deleted ones. */
  @JsonIgnore
  def estLogicalFileSize: Option[Long] = logicalToPhysicalRecordsRatio.map(n => (n * size).toLong)

  /** Returns the ratio of the logical number of records to the total number of records. */
  @JsonIgnore
  def logicalToPhysicalRecordsRatio: Option[Double] = {
    numLogicalRecords.map(numLogicalRecords =>
      numLogicalRecords.toDouble / (numLogicalRecords + numDeletedRecords).toDouble)
  }

  /** Returns the ratio of number of deleted records to the total number of records. */
  @JsonIgnore
  def deletedToPhysicalRecordsRatio: Option[Double] = logicalToPhysicalRecordsRatio.map(1.0d - _)

  @JsonIgnore
  @transient
  lazy val tightBounds: Option[Boolean] = parsedStatsFields.flatMap(_.tightBounds)
}

object AddFile {
  /**
   * Misc file-level metadata.
   *
   * The convention is that clients may safely ignore any/all of these tags and this should never
   * have an impact on correctness.
   *
   * Otherwise, the information should go as a field of the AddFile action itself and the Delta
   * protocol version should be bumped.
   */
  object Tags {
    sealed abstract class KeyType(val name: String)

    /** [[ZCUBE_ID]]: identifier of the OPTIMIZE ZORDER BY job that this file was produced by */
    object ZCUBE_ID extends AddFile.Tags.KeyType("ZCUBE_ID")

    /** [[ZCUBE_ZORDER_BY]]: ZOrdering of the corresponding ZCube */
    object ZCUBE_ZORDER_BY extends AddFile.Tags.KeyType("ZCUBE_ZORDER_BY")

    /** [[ZCUBE_ZORDER_CURVE]]: Clustering strategy of the corresponding ZCube */
    object ZCUBE_ZORDER_CURVE extends AddFile.Tags.KeyType("ZCUBE_ZORDER_CURVE")

    /**
     * [[INSERTION_TIME]]: the latest timestamp in micro seconds when the data in the file
     * was inserted
     */
    object INSERTION_TIME extends AddFile.Tags.KeyType("INSERTION_TIME")


    /** [[PARTITION_ID]]: rdd partition id that has written the file, will not be stored in the
     physical log, only used for communication  */
    object PARTITION_ID extends AddFile.Tags.KeyType("PARTITION_ID")

    /** [[OPTIMIZE_TARGET_SIZE]]: target file size the file was optimized to. */
    object OPTIMIZE_TARGET_SIZE extends AddFile.Tags.KeyType("OPTIMIZE_TARGET_SIZE")

  }

  /** Convert a [[Tags.KeyType]] to a string to be used in the AddMap.tags Map[String, String]. */
  def tag(tagKey: Tags.KeyType): String = tagKey.name
}

/**
 * Logical removal of a given file from the reservoir. Acts as a tombstone before a file is
 * deleted permanently.
 *
 * Note that for protocol compatibility reasons, the fields `partitionValues`, `size`, and `tags`
 * are only present when the extendedFileMetadata flag is true. New writers should generally be
 * setting this flag, but old writers (and FSCK) won't, so readers must check this flag before
 * attempting to consume those values.
 *
 * Since old tables would not have `extendedFileMetadata` and `size` field, we should make them
 * nullable by setting their type Option.
 */
// scalastyle:off
case class RemoveFile(
    override val path: String,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    deletionTimestamp: Option[Long],
    override val dataChange: Boolean = true,
    extendedFileMetadata: Option[Boolean] = None,
    partitionValues: Map[String, String] = null,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    size: Option[Long] = None,
    override val tags: Map[String, String] = null,
    deletionVector: DeletionVectorDescriptor = null
) extends FileAction {
  override def wrap: SingleAction = SingleAction(remove = this)

  @JsonIgnore
  val delTimestamp: Long = deletionTimestamp.getOrElse(0L)

  /** The number of records contained inside the removed file. */
  @JsonIgnore
  var numLogicalRecords: Option[Long] = None

  /** Returns the approx size of the remaining records after excluding the deleted ones. */
  @JsonIgnore
  var estLogicalFileSize: Option[Long] = None

  /**
   * Return the unique id of the deletion vector, if present, or `None` if there's no DV.
   *
   * The unique id differentiates DVs, even if there are multiple in the same file
   * or the DV is stored inline.
   */
  @JsonIgnore
  def getDeletionVectorUniqueId: Option[String] = Option(deletionVector).map(_.uniqueId)

  /** Returns the number of records marked as deleted. */
  @JsonIgnore
  def numDeletedRecords: Long = if (deletionVector != null) deletionVector.cardinality else 0L

  /** Returns the total number of records, including those marked as deleted. */
  @JsonIgnore
  def numPhysicalRecords: Option[Long] = numLogicalRecords.map(_ + numDeletedRecords)

  /**
   * Create a copy with the new tag. `extendedFileMetadata` is copied unchanged.
   */
  def copyWithTag(tag: String, value: String): RemoveFile = copy(
    tags = Option(tags).getOrElse(Map.empty) + (tag -> value))

  /**
   * Create a copy without the tag.
   */
  def copyWithoutTag(tag: String): RemoveFile =
    copy(tags = Option(tags).getOrElse(Map.empty) - tag)

  @JsonIgnore
  override def getFileSize: Long = size.getOrElse(0L)

}
// scalastyle:on

/**
 * A change file containing CDC data for the Delta version it's within. Non-CDC readers should
 * ignore this, CDC readers should scan all ChangeFiles in a version rather than computing
 * changes from AddFile and RemoveFile actions.
 */
case class AddCDCFile(
    override val path: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    override val tags: Map[String, String] = null) extends FileAction {
  override val dataChange = false

  override def wrap: SingleAction = SingleAction(cdc = this)

  @JsonIgnore
  override def getFileSize: Long = size

  @JsonIgnore
  override def numLogicalRecords: Option[Long] = None
}


case class Format(
    provider: String = "parquet",
    // If we support `options` in future, we should not store any file system options since they may
    // contain credentials.
    options: Map[String, String] = Map.empty)

/**
 * Updates the metadata of the table. Only the last update to the [[Metadata]]
 * of a table is kept. It is the responsibility of the writer to ensure that
 * any data already present in the table is still valid after any change.
 */
case class Metadata(
    id: String = if (Utils.isTesting) "testId" else java.util.UUID.randomUUID().toString,
    name: String = null,
    description: String = null,
    format: Format = Format(),
    schemaString: String = null,
    partitionColumns: Seq[String] = Nil,
    configuration: Map[String, String] = Map.empty,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    createdTime: Option[Long] = None) extends Action {

  // The `schema` and `partitionSchema` methods should be vals or lazy vals, NOT
  // defs, because parsing StructTypes from JSON is extremely expensive and has
  // caused perf. problems here in the past:

  /**
   * Column mapping mode for this table
   */
  @JsonIgnore
  lazy val columnMappingMode: DeltaColumnMappingMode =
    DeltaConfigs.COLUMN_MAPPING_MODE.fromMetaData(this)

  /**
   * Column mapping max id for this table
   */
  @JsonIgnore
  lazy val columnMappingMaxId: Long =
    DeltaConfigs.COLUMN_MAPPING_MAX_ID.fromMetaData(this)

  /** Returns the schema as a [[StructType]] */
  @JsonIgnore
  lazy val schema: StructType = Option(schemaString)
    .map(DataType.fromJson(_).asInstanceOf[StructType])
    .getOrElse(StructType.apply(Nil))

  /** Returns the partitionSchema as a [[StructType]] */
  @JsonIgnore
  lazy val partitionSchema: StructType =
    new StructType(partitionColumns.map(c => schema(c)).toArray)

  /** Partition value keys in the AddFile map. */
  @JsonIgnore
  lazy val physicalPartitionSchema: StructType = DeltaColumnMapping.renameColumns(partitionSchema)

  /** Columns written out to files. */
  @JsonIgnore
  lazy val dataSchema: StructType = {
    val partitions = partitionColumns.toSet
    StructType(schema.filterNot(f => partitions.contains(f.name)))
  }

  /** Partition value written out to files */
  @JsonIgnore
  lazy val physicalPartitionColumns: Seq[String] = physicalPartitionSchema.fieldNames.toSeq

  /**
   * Columns whose type should never be changed. For example, if a column is used by a generated
   * column, changing its type may break the constraint defined by the generation expression. Hence,
   * we should never change its type.
   */
  @JsonIgnore
  lazy val fixedTypeColumns: Set[String] =
    GeneratedColumn.getGeneratedColumnsAndColumnsUsedByGeneratedColumns(schema)

  /**
   * Store non-partition columns and their corresponding [[OptimizablePartitionExpression]] which
   * can be used to create partition filters from data filters of these non-partition columns.
   */
  @JsonIgnore
  lazy val optimizablePartitionExpressions: Map[String, Seq[OptimizablePartitionExpression]]
  = GeneratedColumn.getOptimizablePartitionExpressions(schema, partitionSchema)

  override def wrap: SingleAction = SingleAction(metaData = this)
}

/**
 * Interface for objects that represents the information for a commit. Commits can be referred to
 * using a version and timestamp. The timestamp of a commit comes from the remote storage
 * `lastModifiedTime`, and can be adjusted for clock skew. Hence we have the method `withTimestamp`.
 */
trait CommitMarker {
  /** Get the timestamp of the commit as millis after the epoch. */
  def getTimestamp: Long
  /** Return a copy object of this object with the given timestamp. */
  def withTimestamp(timestamp: Long): CommitMarker
  /** Get the version of the commit. */
  def getVersion: Long
}

/**
 * Holds provenance information about changes to the table. This [[Action]]
 * is not stored in the checkpoint and has reduced compatibility guarantees.
 * Information stored in it is best effort (i.e. can be falsified by the writer).
 *
 * @param isBlindAppend Whether this commit has blindly appended without caring about existing files
 * @param engineInfo The information for the engine that makes the commit.
 *                   If a commit is made by Delta Lake 1.1.0 or above, it will be
 *                   `Apache-Spark/x.y.z Delta-Lake/x.y.z`.
 */
case class CommitInfo(
    // The commit version should be left unfilled during commit(). When reading a delta file, we can
    // infer the commit version from the file name and fill in this field then.
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    version: Option[Long],
    timestamp: Timestamp,
    userId: Option[String],
    userName: Option[String],
    operation: String,
    @JsonSerialize(using = classOf[JsonMapSerializer])
    operationParameters: Map[String, String],
    job: Option[JobInfo],
    notebook: Option[NotebookInfo],
    clusterId: Option[String],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    readVersion: Option[Long],
    isolationLevel: Option[String],
    isBlindAppend: Option[Boolean],
    operationMetrics: Option[Map[String, String]],
    userMetadata: Option[String],
    tags: Option[Map[String, String]],
    engineInfo: Option[String],
    txnId: Option[String]) extends Action with CommitMarker {
  override def wrap: SingleAction = SingleAction(commitInfo = this)

  override def withTimestamp(timestamp: Long): CommitInfo = {
    this.copy(timestamp = new Timestamp(timestamp))
  }

  override def getTimestamp: Long = timestamp.getTime
  @JsonIgnore
  override def getVersion: Long = version.get

}

case class JobInfo(
    jobId: String,
    jobName: String,
    runId: String,
    jobOwnerId: String,
    triggerType: String)

object JobInfo {
  def fromContext(context: Map[String, String]): Option[JobInfo] = {
    context.get("jobId").map { jobId =>
      JobInfo(
        jobId,
        context.get("jobName").orNull,
        context.get("runId").orNull,
        context.get("jobOwnerId").orNull,
        context.get("jobTriggerType").orNull)
    }
  }
}

case class NotebookInfo(notebookId: String)

object NotebookInfo {
  def fromContext(context: Map[String, String]): Option[NotebookInfo] = {
    context.get("notebookId").map { nbId => NotebookInfo(nbId) }
  }
}

object CommitInfo {
  def empty(version: Option[Long] = None): CommitInfo = {
    CommitInfo(version, null, None, None, null, null, None, None,
      None, None, None, None, None, None, None, None, None)
  }

  // scalastyle:off argcount
  def apply(
      time: Long,
      operation: String,
      operationParameters: Map[String, String],
      commandContext: Map[String, String],
      readVersion: Option[Long],
      isolationLevel: Option[String],
      isBlindAppend: Option[Boolean],
      operationMetrics: Option[Map[String, String]],
      userMetadata: Option[String],
      tags: Option[Map[String, String]],
      txnId: Option[String]): CommitInfo = {

    val getUserName = commandContext.get("user").flatMap {
      case "unknown" => None
      case other => Option(other)
    }

    CommitInfo(
      None,
      new Timestamp(time),
      commandContext.get("userId"),
      getUserName,
      operation,
      operationParameters,
      JobInfo.fromContext(commandContext),
      NotebookInfo.fromContext(commandContext),
      commandContext.get("clusterId"),
      readVersion,
      isolationLevel,
      isBlindAppend,
      operationMetrics,
      userMetadata,
      tags,
      getEngineInfo,
      txnId)
  }
  // scalastyle:on argcount

  private def getEngineInfo: Option[String] = {
    Some(s"Apache-Spark/${org.apache.spark.SPARK_VERSION} Delta-Lake/${io.delta.VERSION}")
  }

}

/** A serialization helper to create a common action envelope. */
case class SingleAction(
    txn: SetTransaction = null,
    add: AddFile = null,
    remove: RemoveFile = null,
    metaData: Metadata = null,
    protocol: Protocol = null,
    cdc: AddCDCFile = null,
    commitInfo: CommitInfo = null) {

  def unwrap: Action = {
    if (add != null) {
      add
    } else if (remove != null) {
      remove
    } else if (metaData != null) {
      metaData
    } else if (txn != null) {
      txn
    } else if (protocol != null) {
      protocol
    } else if (cdc != null) {
      cdc
    } else if (commitInfo != null) {
      commitInfo
    } else {
      null
    }
  }
}

object SingleAction extends Logging {
  implicit def encoder: Encoder[SingleAction] =
    org.apache.spark.sql.delta.implicits.singleActionEncoder

  implicit def addFileEncoder: Encoder[AddFile] =
    org.apache.spark.sql.delta.implicits.addFileEncoder

  lazy val nullLitForRemoveFile: Column =
    new Column(Literal(null, ScalaReflection.schemaFor[RemoveFile].dataType))

  lazy val nullLitForAddCDCFile: Column =
    new Column(Literal(null, ScalaReflection.schemaFor[AddCDCFile].dataType))

  lazy val nullLitForMetadataAction: Column =
    new Column(Literal(null, ScalaReflection.schemaFor[Metadata].dataType))
}

/** Serializes Maps containing JSON strings without extra escaping. */
class JsonMapSerializer extends JsonSerializer[Map[String, String]] {
  def serialize(
      parameters: Map[String, String],
      jgen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    jgen.writeStartObject()
    parameters.foreach { case (key, value) =>
      if (value == null) {
        jgen.writeNullField(key)
      } else {
        jgen.writeFieldName(key)
        // Write value as raw data, since it's already JSON text
        jgen.writeRawValue(value)
      }
    }
    jgen.writeEndObject()
  }
}
