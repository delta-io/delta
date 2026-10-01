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

package org.apache.spark.sql.delta.uniform

import java.nio.ByteBuffer

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaColumnMapping._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.icebergShaded._
import org.apache.spark.sql.delta.util.JsonUtils
import shadedForDelta.org.apache.iceberg._
import shadedForDelta.org.apache.iceberg.hadoop.HadoopTables
import shadedForDelta.org.apache.iceberg.types._
import shadedForDelta.org.apache.iceberg.util.DateTimeUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.types._

/**
 * Verify that Delta tables match the UniForm Iceberg tables converted from them.
 * NOTE: this method should avoid using existing conversion utils as much as possible, so that
 *       we have a pair of fresh eyes reviewing the behaviors of UniForm.
 */
class UniFormIcebergVerifier(
     spark: SparkSession,
     deltaLog: DeltaLog,
     catalogTableOpt: Option[CatalogTable],
     icebergTable: Table) {

  def this(spark: SparkSession, catalogTable: CatalogTable, icebergTable: Table) =
    this(spark, DeltaLog.forTable(spark, catalogTable), Some(catalogTable), icebergTable)

  def this(spark: SparkSession, tableName: String,
           loadIceberg: String => Table = UniFormIcebergVerifier.loadIcebergTableFromUC) =
    this(
      spark,
      spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)),
      loadIceberg.apply(tableName)
    )

  val compatObject: IcebergCompatBase = IcebergCompat
    .anyEnabled(deltaLog.update(catalogTableOpt = catalogTableOpt).metadata)
    .orNull

  val deltaSnapshot = deltaLog.update(catalogTableOpt = catalogTableOpt)
  val deltaSchema = deltaSnapshot.schema
  val icebergSchema = icebergTable.schema()

  /* =====================================
   *  Check Switches that can be overriden
   * ====================================== */
  protected var enforceFieldIdCheck = true

  protected var enforceTableSizeCheck = true

  protected var enforceSnapshotSeqNumCheck = true
  protected var expectedSnapshotSeqNumber: Option[Int] = None

  protected var enforceDataFileStatsCheck = true


  def withCompatVersion(condition: Int => Boolean)(f: => Unit): Unit = {
    if (compatObject != null && condition.apply(compatObject.version)) {
      f
    }
  }

  def verify(): Unit = {
    verifyMetadata()
    verifyContent()
  }

  protected def verifyMetadata(): Unit = {
    verifySchema()
    verifyProperties()
    verifyTableSummary()
    verifyDataFiles()
  }

  protected def verifySchema(): Unit = {
    verifySchemaFields()
    verifyPartition()
  }

  private var propertiesToVerify: Seq[(String, String)] = Seq.empty

  def includeProperties(props: Seq[(String, String)]): UniFormIcebergVerifier = {
    propertiesToVerify = props
    this
  }

  def assertSnapshotSeqNum(
        expectedSnapshotSeqNumber: Int): UniFormIcebergVerifier = {
    this.expectedSnapshotSeqNumber = Some(expectedSnapshotSeqNumber)
    this
  }

  def bypassSnapshotSeqNumCheck(): UniFormIcebergVerifier = {
    this.enforceSnapshotSeqNumCheck = false
    this
  }

  def bypassDataFileStatsCheck(): UniFormIcebergVerifier = {
    this.enforceDataFileStatsCheck = false
    this
  }

  protected def verifyProperties(): Unit = {
    this.propertiesToVerify.foreach { case (key, value) =>
      val icebergValue = icebergTable.properties().get(key)
      val deltaValue = deltaSnapshot.metadata.configuration.get(key)
      key match {
        case deltaKey if key.startsWith("delta") =>
          assert(deltaValue.contains(value), key)
        case formatVersionKey if key == "format-version" =>
          val formatVersion = icebergTable.asInstanceOf[BaseTable]
            .operations().current().formatVersion()
          assert(deltaValue.contains(s"$formatVersion"))
        case _ =>
          assert(deltaValue.contains(icebergValue),
            s"$key: delta -> $deltaValue, iceberg -> $icebergValue")
      }
    }
  }

  protected def verifySchemaFields(): Unit = {
    // Flatten Iceberg Type to a map of path to field
    def flattenType(rootType: Type): Map[Seq[String], Types.NestedField] = {
      rootType match {
        case struct: Types.StructType =>
          struct
            .fields()
            .asScala
            .flatMap(
              f =>
                flattenType(f.`type`()) match {
                  case empty if empty.isEmpty => Map(Seq(f.name) -> f)
                  case nonempty =>
                    nonempty.map { case (key, field) => (f.name +: key, field) }.toMap
                }
            )
            .toMap
        case map: Types.MapType =>
          val keyMap = flattenType(map.keyType()) match {
            case empty if empty.isEmpty =>
              Map(Seq("key") -> Types.NestedField.required(map.keyId(), "key", map.keyType()))
            case nonempty => nonempty.map { case (key, field) => ("key" +: key, field) }.toMap
          }
          val valueMap = flattenType(map.valueType()) match {
            case empty if empty.isEmpty =>
              Map(
                Seq("value") -> Types.NestedField
                  .of(map.valueId(), map.isValueOptional, "value", map.valueType())
              )
            case nonempty => nonempty.map { case (key, field) => ("value" +: key, field) }.toMap
          }
          keyMap ++ valueMap
        case list: Types.ListType =>
          flattenType(list.elementType()) match {
            case empty if empty.isEmpty =>
              Map(
                Seq("element") -> Types.NestedField
                  .of(list.elementId(), list.isElementOptional, "element", list.elementType())
              )
            case nonempty => nonempty.map { case (key, field) => ("element" +: key, field) }.toMap
          }
        case _ => Map.empty
      }
    }

    val pathToIcebergFields = flattenType(icebergSchema.asStruct())
    pathToIcebergFields.foreach {
      case (path, icebergField) =>
        val deltaField = deltaSchema.findNestedField(path, includeCollections = true).get._2
        if (deltaField.metadata.contains(COLUMN_MAPPING_METADATA_ID_KEY) && enforceFieldIdCheck) {
          assert(deltaField.metadata.getLong(COLUMN_MAPPING_METADATA_ID_KEY)
            == icebergField.fieldId())
        }
        verifySchemaField(deltaField, icebergField)
    }
  }

  protected def verifySchemaField(
      deltaField: StructField,
      icebergField: Types.NestedField): Unit = {
    assert(deltaField.name == icebergField.name())
    assert(deltaField.nullable == icebergField.isOptional)

    val icebergType = deltaField.dataType match {
      case VariantType => Types.VariantType.get()
      case _ => IcebergSchemaUtils.convertAtomic(deltaField.dataType)
    }
    assert(icebergType == icebergField.`type`())

    withCompatVersion(_ >= 3) {
      verifyColumnDefaults(deltaField, icebergField)
    }
  }

  protected def verifyColumnDefaults(
      deltaField: StructField, icebergField: Types.NestedField): Unit = {
    val deltaDefaultValue = deltaField.getCurrentDefaultValue()
    val icebergDefault = Option(icebergField.writeDefault()).map(_.toString)
    assert(deltaDefaultValue == icebergDefault)
  }

  protected def verifyPartition(): Unit = {
    val icebergPartition = icebergTable.spec()
    val deltaPartCols = deltaLog.update(
      catalogTableOpt = catalogTableOpt).metadata.partitionColumns
    assert(deltaPartCols.nonEmpty == icebergPartition.isPartitioned)
    deltaPartCols.foreach { p =>
      assert(icebergPartition.schema().findField(p) != null)
    }
  }

  protected def verifyTableSummary(): Unit = {
    // Verify statistics consistency
    val deltaSnapshot = deltaLog.update(catalogTableOpt = catalogTableOpt)
    val deltaState = deltaSnapshot.computeChecksum
    // Delta table size does not count in dv size
    val dvFileSize = deltaSnapshot.allFiles
      .collect()
      .map(a => Option(a.deletionVector).map(_.sizeInBytes + 8).getOrElse(0))
      .sum
    if (icebergTable.currentSnapshot() != null) {
      val currentSnapshotSummary = icebergTable.currentSnapshot().summary()
      if (enforceTableSizeCheck) {
        assert(
          currentSnapshotSummary.get("total-files-size").toLong ==
          deltaState.tableSizeBytes + dvFileSize,
          "Total files size should match")
        assert(
          currentSnapshotSummary.get("total-data-files").toLong == deltaState.numFiles,
          "Number of files should match")
      }
    }
  }

  protected def verifyDataFiles(): Unit = {
    val deltaAllFiles = deltaLog.update(catalogTableOpt = catalogTableOpt).allFiles.collect()
    val icebergAllFiles: Seq[DataFile] =
      Option(icebergTable.currentSnapshot())
        .map(_.allManifests(icebergTable.io())
          .asScala
          .flatMap { manifest =>
            manifest.content() match {
              case ManifestContent.DATA =>
                ManifestFiles.read(manifest, icebergTable.io()).asScala.toSeq
              case _ => Seq.empty
            }
          }
          .toSeq)
        .getOrElse(Seq.empty)
    assert(deltaAllFiles.length == icebergAllFiles.size)
    val deltaPathToFile = deltaAllFiles
      .map(file => (file.absolutePath(deltaLog).toString -> file))
      .toMap
    val icebergPathToFile = icebergAllFiles.map(file => (file.path() -> file)).toMap
    assert(deltaPathToFile.keySet == icebergPathToFile.keySet)
    deltaPathToFile.keySet.foreach { key =>
      verifyDataFile(deltaPathToFile(key), icebergPathToFile(key))
    }

    withCompatVersion(_ >= 3) {
      verifyDeletionVectors()
      verifyRowTrackingMetadata()
    }
  }

  protected def verifyDeletionVectors(): Unit = {
    val icebergDeleteFiles =
      Option(icebergTable.currentSnapshot())
        .map(
          _.deleteManifests(icebergTable.io())
            .asScala
            .flatMap(ManifestFiles.readDeleteManifest(_, icebergTable.io(), null).asScala.toSeq)
            .map(df => (df.referencedDataFile, df.path))
            .toMap)
        .getOrElse(Map.empty)

    val deltaLogDeleteFiles = deltaLog.update(
        catalogTableOpt = catalogTableOpt).allFiles.collect().collect {
      case add: AddFile if add.deletionVector != null =>
        (add.absolutePath(deltaLog).toString,
          add.deletionVector.absolutePath(deltaLog.dataPath).toString)
    }.toMap
    assert(icebergDeleteFiles == deltaLogDeleteFiles)
  }

  protected def verifyRowTrackingMetadata(): Unit = {
    if (icebergTable.currentSnapshot() == null) {
      return
    }
    // verify high water mark equal to next row id
    val icebergNextRowId = icebergTable.asInstanceOf[BaseTable].operations().refresh().nextRowId()
    val deltaHighWaterMark = RowId.extractHighWatermark(
      deltaLog.update(catalogTableOpt = catalogTableOpt)).get
    assert(icebergNextRowId == deltaHighWaterMark + 1)

    if (this.expectedSnapshotSeqNumber.nonEmpty) {
      assert(this.expectedSnapshotSeqNumber.get
        == icebergTable.currentSnapshot().sequenceNumber(),
        s"expected Snapshot Seq Number ${expectedSnapshotSeqNumber.get}, " +
          s"iceberg sequence ${icebergTable.currentSnapshot().sequenceNumber()}"
      )
    } else if (enforceSnapshotSeqNumCheck) {
      // verify iceberg snapshot sequence number match latest delta version
      assert(deltaLog.update(catalogTableOpt = catalogTableOpt).version ==
        icebergTable.currentSnapshot().sequenceNumber(),
        s"delta log version ${deltaLog.update(catalogTableOpt = catalogTableOpt).version}, " +
          s"iceberg sequence ${icebergTable.currentSnapshot().sequenceNumber()}"
      )
    }
  }

  protected def verifyDataFile(deltaFile: AddFile, icebergFile: DataFile): Unit = {
    verifyDataFilePartition(deltaFile, icebergFile)
    if (enforceDataFileStatsCheck) {
      verifyDataFileStats(deltaFile, icebergFile)
    }
    withCompatVersion( _ >= 3) {
      verifyRowTrackingFileMetadata(deltaFile, icebergFile)
    }
  }

  protected def verifyDataFilePartition(deltaFile: AddFile, icebergFile: DataFile): Unit = {
    // Verify Partition match
    val icebergPartitionSpec = icebergTable.specs.get(icebergFile.specId())

    assert(deltaFile.partitionValues.size == icebergFile.partition().size())

    val logicalToPhysicalForPartCols = IcebergTransactionUtils
      .getPartitionPhysicalNameMapping(deltaSnapshot.metadata.partitionSchema)
    val dataTypeByLogicalName = deltaSchema.fields.map(f => (f.name, f.dataType)).toMap

    for (i <- 0 until icebergPartitionSpec.fields().size()) {
      val icebergPartName = icebergPartitionSpec.fields().get(i).name()
      val icebergPartValue = icebergFile.partition().asInstanceOf[PartitionData].get(i)

      val logicalName = icebergPartName
      val dataType = dataTypeByLogicalName(logicalName)
      val stringVal = deltaFile.partitionValues(logicalToPhysicalForPartCols(logicalName))
      val icebergPartValueFromDelta = IcebergTransactionUtils
        .stringToIcebergPartitionValue(dataType, stringVal, 0)

      assert(icebergPartValue == icebergPartValueFromDelta)
    }
  }

  protected def verifyDataFileStats(deltaFile: AddFile, icebergFile: DataFile): Unit = {
    val deltaStats = JsonUtils.fromJson[Map[String, Any]](deltaFile.stats)
    val deltaNumRecords = deltaStats("numRecords").asInstanceOf[Int]
    assert(deltaNumRecords == icebergFile.recordCount())

    // Translate Iceberg partition data in ByteBuffer into Java/Scala objects
    def deserialize(ftype: Type, value: Any): Any = {
      (ftype, value) match {
        case (_, null) => null
        case (_: Types.StringType, bb: ByteBuffer) =>
          Conversions.fromByteBuffer(ftype, bb).toString
        case (_: Types.DateType, bb: ByteBuffer) =>
          val daysFromEpoch = Conversions.fromByteBuffer(ftype, bb).asInstanceOf[Int]
          DateTimeUtil.dateFromDays(daysFromEpoch).toString
        case (tsType: Types.TimestampType, bb: ByteBuffer) =>
          val microts = Conversions.fromByteBuffer(tsType, bb).asInstanceOf[java.lang.Long]
          if (tsType.shouldAdjustToUTC()) {
            DateTimeUtil.microsToIsoTimestamptz(microts)
          } else {
            DateTimeUtil.microsToIsoTimestamp(microts)
          }
        case (_, bb: java.nio.ByteBuffer) =>
          Conversions.fromByteBuffer(ftype, bb)
        case _ => throw new IllegalArgumentException("unable to deserialize unknown values")
      }
    }
    // Collect a mapping of Delta physical name to field id
    def collectIdToPhysicalName(root: StructType): Map[Int, String] =
      root.fields.flatMap { field =>
        val fieldPair = field.metadata.getLong(COLUMN_MAPPING_METADATA_ID_KEY).toInt ->
          field.metadata.getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
        val subPairs = field.dataType match {
          case struct: StructType => collectIdToPhysicalName(struct).toSeq
          case list: ArrayType =>
            list.elementType match {
              case listStruct: StructType => collectIdToPhysicalName(listStruct).toSeq
              case _ => Nil
            }
          case map: MapType =>
            val mapKeyPairs = map.keyType match {
              case keyStruct: StructType => collectIdToPhysicalName(keyStruct).toSeq
              case _ => Nil
            }
            val mapValuePairs = map.valueType match {
              case valueStruct: StructType => collectIdToPhysicalName(valueStruct).toSeq
              case _ => Nil
            }
            mapKeyPairs ++ mapValuePairs
          case _ => Nil
        }
        subPairs :+ fieldPair
      }.toMap
    // Make leveled Delta stats a flat map
    def flattenDeltaStats(input: Map[String, Any]): Map[String, Any] =
      input.flatMap {
        case (key, value) =>
          value match {
            case nestedMap: Map[String, Any] => flattenDeltaStats(nestedMap).toSeq
            case _ => Seq(key -> value)
          }
      }.toMap

    val idToPhysicalName = collectIdToPhysicalName(deltaSchema)
    val physicalNames = idToPhysicalName.values.toSet

    // Filter out fields no longer in the latest schema
    val deltaMax = flattenDeltaStats(deltaStats("maxValues").asInstanceOf[Map[String, Any]])
      .filter { case (key, _) => physicalNames.contains(key) }
    val icebergMax = icebergFile
      .upperBounds()
      .asScala
      .collect {
        case (id, value) if idToPhysicalName.contains(id) =>
          idToPhysicalName(id) -> deserialize(icebergSchema.findField(id).`type`(), value)
      }
      .toMap
    assert(icebergMax == deltaMax)

    val deltaMin = flattenDeltaStats(deltaStats("minValues").asInstanceOf[Map[String, Any]])
      .filter { case (key, _) => physicalNames.contains(key) }
    val icebergMin = icebergFile
      .lowerBounds()
      .asScala
      .collect {
        case (id, value) if idToPhysicalName.contains(id) =>
          idToPhysicalName(id) -> deserialize(icebergSchema.findField(id).`type`(), value)
      }
      .toMap
    assert(icebergMin == deltaMin)

    val deltaNullCount = flattenDeltaStats(deltaStats("nullCount").asInstanceOf[Map[String, Any]])
      .filter { case (key, _) => physicalNames.contains(key) }
    val icebergNullCount = icebergFile
      .nullValueCounts()
      .asScala
      .collect {
        case (id, value) if idToPhysicalName.contains(id) =>
          idToPhysicalName(id) -> value
      }
      .toMap
    assert(icebergNullCount == deltaNullCount)
  }

  protected def verifyRowTrackingFileMetadata(deltaFile: AddFile, icebergFile: DataFile): Unit = {
    assert(deltaFile.baseRowId.getOrElse(null.asInstanceOf[Long])
      == icebergFile.firstRowId(), s"delta row id ${deltaFile.baseRowId}," +
      s"iceberg row id ${icebergFile.firstRowId()}")
  }

  protected def verifyContent(): Unit = {
    // Do nothing for now
  }
}

object UniFormIcebergVerifier {
  /**
   * Load Iceberg table from a metadata path
   */
  def loadIcebergTableFromPath(spark: SparkSession, metadataLocation: String): Table = {
    // scalastyle:off deltahadoopconfiguration
    val hadoopTables = new HadoopTables(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    hadoopTables.load(metadataLocation)
  }
  /**
   * Load Iceberg table from UC.
   */
  def loadIcebergTableFromUC(tableName: String): Table =
    SparkSession.getActiveSession.flatMap { spark =>
      val tableMetadata = spark.sessionState.catalog
        .getTableMetadata(TableIdentifier(tableName))
      tableMetadata.storage.properties
        .get(IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP)
        .map(tablePath => loadIcebergTableFromPath(spark, tablePath))
    }.getOrElse(throw new IllegalArgumentException("Spark Session not available"))
}
