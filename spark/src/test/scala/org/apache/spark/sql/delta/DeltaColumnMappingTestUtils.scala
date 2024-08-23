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

import java.io.File

import scala.collection.mutable

import org.apache.spark.sql.delta.actions.{Metadata, Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaColumnMappingSelectedTestMixin
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import io.delta.tables.{DeltaTable => OSSDeltaTable}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.ColumnImplicitsShim._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{AtomicType, StructField, StructType}

trait DeltaColumnMappingTestUtilsBase extends SharedSparkSession {

  import testImplicits._

  protected def columnMappingMode: String = NoMapping.name

  private val PHYSICAL_NAME_REGEX =
    "col-[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}".r

  implicit class PhysicalNameString(s: String) {
    def phy(deltaLog: DeltaLog): String = {
      PHYSICAL_NAME_REGEX
        .findFirstIn(s)
        .getOrElse(getPhysicalName(s, deltaLog))
    }
  }

  protected def columnMappingEnabled: Boolean = {
    columnMappingModeString != "none"
  }

  protected def columnMappingModeString: String = {
    spark.conf.getOption(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey)
      .getOrElse("none")
  }

  /**
   * Check if two schemas are equal ignoring column mapping metadata
   * @param schema1 Schema
   * @param schema2 Schema
   */
  protected def assertEqual(schema1: StructType, schema2: StructType): Unit = {
    if (columnMappingEnabled) {
      assert(
        DeltaColumnMapping.dropColumnMappingMetadata(schema1) ==
        DeltaColumnMapping.dropColumnMappingMetadata(schema2)
      )
    } else {
      assert(schema1 == schema2)
    }
  }

  /**
   * Check if two table configurations are equal ignoring column mapping metadata
   * @param config1 Table config
   * @param config2 Table config
   */
  protected def assertEqual(
      config1: Map[String, String],
      config2: Map[String, String]): Unit = {
    if (columnMappingEnabled) {
      assert(dropColumnMappingConfigurations(config1) == dropColumnMappingConfigurations(config2))
    } else {
      assert(config1 == config2)
    }
  }

  /**
   * Check if a partition with specific values exists.
   * Handles both column mapped and non-mapped cases
   * @param partCol Partition column name
   * @param partValue Partition value
   * @param deltaLog DeltaLog
   */
  protected def assertPartitionWithValueExists(
      partCol: String,
      partValue: String,
      deltaLog: DeltaLog): Unit = {
    assert(getPartitionFilePathsWithValue(partCol, partValue, deltaLog).nonEmpty)
  }

  /**
   * Assert partition exists in an array of set of partition names/paths
   * @param partCol Partition column name
   * @param deltaLog Delta log
   * @param inputFiles Input files to scan for DF
   */
  protected def assertPartitionExists(
      partCol: String,
      deltaLog: DeltaLog,
      inputFiles: Array[String]): Unit = {
    val physicalName = partCol.phy(deltaLog)
    val allFiles = deltaLog.update().allFiles.collect()
    // NOTE: inputFiles are *not* URL-encoded.
    val filesWithPartitions = inputFiles.map { f =>
      allFiles.filter { af =>
        f.contains(af.toPath.toString)
      }.flatMap(_.partitionValues.keys).toSet
    }
    assert(filesWithPartitions.forall(p => p.count(_ == physicalName) > 0))
    // for non-column mapped mode, we can check the file paths as well
    if (!columnMappingEnabled) {
      assert(inputFiles.forall(path => path.contains(s"$physicalName=")),
          s"${inputFiles.toSeq.mkString("\n")}\ndidn't contain partition columns $physicalName")
    }
  }

  /**
   * Load Deltalog from path
   * @param pathOrIdentifier Location
   * @param isIdentifier Whether the previous argument is a metastore identifier
   * @return
   */
  protected def loadDeltaLog(pathOrIdentifier: String, isIdentifier: Boolean = false): DeltaLog = {
    if (isIdentifier) {
      DeltaLog.forTable(spark, TableIdentifier(pathOrIdentifier))
    } else {
      DeltaLog.forTable(spark, pathOrIdentifier)
    }
  }

  /**
   * Convert a (nested) column string to sequence of name parts
   * @param col Column string
   * @return Sequence of parts
   */
  protected def columnNameToParts(col: String): Seq[String] = {
    UnresolvedAttribute.parseAttributeName(col)
  }

  /**
   * Get partition file paths for a specific partition value
   * @param partCol Logical or physical partition name
   * @param partValue Partition value
   * @param deltaLog DeltaLog
   * @return List of paths
   */
  protected def getPartitionFilePathsWithValue(
      partCol: String,
      partValue: String,
      deltaLog: DeltaLog): Array[String] = {
    getPartitionFilePaths(partCol, deltaLog).getOrElse(partValue, Array.empty)
  }

  /**
   * Get the partition value for null
   */
  protected def nullPartitionValue: String = {
    if (columnMappingEnabled) {
      null
    } else {
      ExternalCatalogUtils.DEFAULT_PARTITION_NAME
    }
  }

  /**
   * Get partition file paths grouped by partition value
   * @param partCol Logical or physical partition name
   * @param deltaLog DeltaLog
   * @return Partition value to paths
   */
  protected def getPartitionFilePaths(
      partCol: String,
      deltaLog: DeltaLog): Map[String, Array[String]] = {
    if (columnMappingEnabled) {
      val colName = partCol.phy(deltaLog)
      deltaLog.update().allFiles.collect()
        .groupBy(_.partitionValues(colName))
        .mapValues(_.map(deltaLog.dataPath.toUri.getPath + "/" + _.path)).toMap
    } else {
      val partColEscaped = s"${ExternalCatalogUtils.escapePathName(partCol)}"
      val dataPath = new File(deltaLog.dataPath.toUri.getPath)
      dataPath.listFiles().filter(_.getName.startsWith(s"$partColEscaped="))
        .groupBy(_.getName.split("=").last).mapValues(_.map(_.getPath)).toMap
    }
  }

  /**
   * Group a list of input file paths by partition key-value pair w.r.t. delta log
   * @param inputFiles Input file paths
   * @param deltaLog Delta log
   * @return A mapped array each with the corresponding partition keys
   */
  protected def groupInputFilesByPartition(
      inputFiles: Array[String],
      deltaLog: DeltaLog): Map[(String, String), Array[String]] = {
    if (columnMappingEnabled) {
      val allFiles = deltaLog.update().allFiles.collect()
      val grouped = inputFiles.flatMap { f =>
        allFiles.find {
          af => f.contains(af.toPath.toString)
        }.head.partitionValues.map(entry => (f, entry))
      }.groupBy(_._2)
      grouped.mapValues(_.map(_._1)).toMap
    } else {
      inputFiles.groupBy(p => {
        val nameParts = new Path(p).getParent.getName.split("=")
        (nameParts(0), nameParts(1))
      })
    }
  }

  /**
   * Drop column mapping configurations from Map
   * @param configuration Table configuration
   * @return Configuration
   */
  protected def dropColumnMappingConfigurations(
      configuration: Map[String, String]): Map[String, String] = {
    configuration - DeltaConfigs.COLUMN_MAPPING_MODE.key - DeltaConfigs.COLUMN_MAPPING_MAX_ID.key
  }

  /**
   * Drop column mapping configurations from Dataset (e.g. sql("SHOW TBLPROPERTIES t1")
   * @param configs Table configuration
   * @return Configuration Dataset
   */
  protected def dropColumnMappingConfigurations(
      configs: Dataset[(String, String)]): Dataset[(String, String)] = {
    spark.createDataset(configs.collect().filter(p =>
      !Seq(
        DeltaConfigs.COLUMN_MAPPING_MAX_ID.key,
        DeltaConfigs.COLUMN_MAPPING_MODE.key
      ).contains(p._1)
    ))
  }

  /** Return KV pairs of Protocol-related stuff for checking the result of DESCRIBE TABLE. */
  protected def buildProtocolProps(snapshot: Snapshot): Seq[(String, String)] = {
    val mergedConf =
      DeltaConfigs.mergeGlobalConfigs(spark.sessionState.conf, snapshot.metadata.configuration)
    val metadata = snapshot.metadata.copy(configuration = mergedConf)
    var props = Seq(
      (Protocol.MIN_READER_VERSION_PROP,
        Protocol.forNewTable(spark, Some(metadata)).minReaderVersion.toString),
      (Protocol.MIN_WRITER_VERSION_PROP,
        Protocol.forNewTable(spark, Some(metadata)).minWriterVersion.toString))
    if (snapshot.protocol.supportsReaderFeatures || snapshot.protocol.supportsWriterFeatures) {
      props ++=
        Protocol.minProtocolComponentsFromAutomaticallyEnabledFeatures(
          spark, metadata, snapshot.protocol)
          ._3
          .map(f => (
            s"${TableFeatureProtocolUtils.FEATURE_PROP_PREFIX}${f.name}",
            TableFeatureProtocolUtils.FEATURE_PROP_SUPPORTED))
    }
    props
  }

  /**
   * Convert (nested) column name string into physical name with reference from DeltaLog
   * If target field does not have physical name, display name is returned
   * @param col Logical column name
   * @param deltaLog Reference DeltaLog
   * @return Physical column name
   */
  protected def getPhysicalName(col: String, deltaLog: DeltaLog): String = {
    val nameParts = UnresolvedAttribute.parseAttributeName(col)
    val realSchema = deltaLog.update().schema
    getPhysicalName(nameParts, realSchema)
  }

  protected def getPhysicalName(col: String, schema: StructType): String = {
    val nameParts = UnresolvedAttribute.parseAttributeName(col)
    getPhysicalName(nameParts, schema)
  }

  protected def getPhysicalName(nameParts: Seq[String], schema: StructType): String = {
    SchemaUtils.findNestedFieldIgnoreCase(schema, nameParts, includeCollections = true)
      .map(DeltaColumnMapping.getPhysicalName)
      .get
  }

  protected def withColumnMappingConf(mode: String)(f: => Any): Any = {
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> mode) {
      f
    }
  }

  protected def withMaxColumnIdConf(maxId: String)(f: => Any): Any = {
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MAX_ID.defaultTablePropertyKey -> maxId) {
      f
    }
  }

  /**
   * Gets the physical names of a path. This is used for converting column paths in stats schema,
   * so it's ok to not support MapType and ArrayType.
   */
  def getPhysicalPathForStats(path: Seq[String], schema: StructType): Option[Seq[String]] = {
    if (path.isEmpty) return Some(Seq.empty)
    val field = schema.fields.find(_.name.equalsIgnoreCase(path.head))
    field match {
      case Some(f @ StructField(_, _: AtomicType, _, _ )) =>
        if (path.size == 1) Some(Seq(DeltaColumnMapping.getPhysicalName(f))) else None
      case Some(f @ StructField(_, st: StructType, _, _)) =>
        val tail = getPhysicalPathForStats(path.tail, st)
        tail.map(DeltaColumnMapping.getPhysicalName(f) +: _)
      case _ =>
        None
    }
  }

   /**
   * Convert (nested) column name string into physical name.
   * Ignore parts of special paths starting with:
   *  1. stats columns: minValues, maxValues, numRecords
   *  2. stats df: stats_parsed
   *  3. partition values: partitionValues_parsed, partitionValues
   * @param col Logical column name (e.g. a.b.c)
   * @param schema Reference schema with metadata
   * @return Unresolved attribute with physical name paths
   */
  protected def convertColumnNameToAttributeWithPhysicalName(
      col: String,
      schema: StructType): UnresolvedAttribute = {
    val parts = UnresolvedAttribute.parseAttributeName(col)
    val shouldIgnoreFirstPart = Set(
      "minValues",
      "maxValues",
      "numRecords",
      Checkpoints.STRUCT_PARTITIONS_COL_NAME,
      "partitionValues")
    val shouldIgnoreSecondPart = Set(Checkpoints.STRUCT_STATS_COL_NAME, "stats")
    val physical = if (shouldIgnoreFirstPart.contains(parts.head)) {
      parts.head +: getPhysicalPathForStats(parts.tail, schema).getOrElse(parts.tail)
    } else if (shouldIgnoreSecondPart.contains(parts.head)) {
      parts.take(2) ++ getPhysicalPathForStats(parts.slice(2, parts.length), schema)
          .getOrElse(parts.slice(2, parts.length))
    } else {
      getPhysicalPathForStats(parts, schema).getOrElse(parts)
    }
    UnresolvedAttribute(physical)
  }

  /**
   * Convert a list of (nested) stats columns into physical name with reference from DeltaLog
   * @param columns Logical columns
   * @param deltaLog Reference DeltaLog
   * @return Physical columns
   */
  protected def convertToPhysicalColumns(
      columns: Seq[Column],
      deltaLog: DeltaLog): Seq[Column] = {
    val schema = deltaLog.update().schema
    columns.map { col =>
      // Implicit `Column.expr` doesn't work due to ambiguity
      // both method ColumnExprExt in object ColumnImplicitsShim of type
      //   (column: org.apache.spark.sql.Column):
      //   org.apache.spark.sql.ColumnImplicitsShim.ColumnExprExt
      // and method toRichColumn in object testImplicits of type
      //   (c: org.apache.spark.sql.Column): org.apache.spark.sql.SparkSession#RichColumn
      val newExpr = expression(col).transform {
        case a: Attribute =>
          convertColumnNameToAttributeWithPhysicalName(a.name, schema)
      }
      Column(newExpr)
    }
  }

  /**
   * Standard CONVERT TO DELTA
   * @param tableOrPath String
   */
  protected def convertToDelta(tableOrPath: String): Unit = {
    sql(s"CONVERT TO DELTA $tableOrPath")
  }

  /**
   * Force enable streaming read (with possible data loss) on column mapping enabled table with
   * drop / rename schema changes.
   */
  protected def withStreamingReadOnColumnMappingTableEnabled(f: => Unit): Unit = {
    if (columnMappingEnabled) {
      withSQLConf(DeltaSQLConf
        .DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_COLUMN_MAPPING_SCHEMA_CHANGES.key -> "true") {
        f
      }
    } else {
      f
    }
  }

}

trait DeltaColumnMappingTestUtils extends DeltaColumnMappingTestUtilsBase

/**
 * Include this trait to enable Id column mapping mode for a suite
 */
trait DeltaColumnMappingEnableIdMode extends SharedSparkSession
  with DeltaColumnMappingTestUtils
  with DeltaColumnMappingSelectedTestMixin {

  protected override def columnMappingMode: String = IdMapping.name

  protected override def sparkConf: SparkConf =
    super.sparkConf.set(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey, "id")

  /**
   * CONVERT TO DELTA blocked in id mode
   */
  protected override def convertToDelta(tableOrPath: String): Unit =
    throw DeltaErrors.convertToDeltaWithColumnMappingNotSupported(
      DeltaColumnMappingMode(columnMappingModeString)
    )
}

/**
 * Include this trait to enable Name column mapping mode for a suite
 */
trait DeltaColumnMappingEnableNameMode extends SharedSparkSession
  with DeltaColumnMappingTestUtils
  with DeltaColumnMappingSelectedTestMixin {

  protected override def columnMappingMode: String = NameMapping.name

  protected override def sparkConf: SparkConf =
    super.sparkConf.set(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey, columnMappingMode)

  /**
   * CONVERT TO DELTA can be possible under name mode in tests
   */
  protected override def convertToDelta(tableOrPath: String): Unit = {
    withColumnMappingConf("none") {
      super.convertToDelta(tableOrPath)
    }

    val (deltaPath, deltaLog) =
      if (tableOrPath.contains("parquet") && tableOrPath.contains("`")) {
        // parquet.`PATH`
        val plainPath = tableOrPath.split('.').last.drop(1).dropRight(1)
        (s"delta.`$plainPath`", DeltaLog.forTable(spark, plainPath))
      } else {
        (tableOrPath, DeltaLog.forTable(spark, TableIdentifier(tableOrPath)))
      }

    val tableReaderVersion = deltaLog.unsafeVolatileSnapshot.protocol.minReaderVersion
    val tableWriterVersion = deltaLog.unsafeVolatileSnapshot.protocol.minWriterVersion
    val requiredReaderVersion = if (tableWriterVersion >=
      TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION) {
      // If the writer version of the table supports table features, we need to
      // bump the reader version to table features to enable column mapping.
      TableFeatureProtocolUtils.TABLE_FEATURES_MIN_READER_VERSION
    } else {
      ColumnMappingTableFeature.minReaderVersion
    }
    val readerVersion = spark.conf.get(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION).max(
      requiredReaderVersion)
    val writerVersion = spark.conf.get(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION).max(
      ColumnMappingTableFeature.minWriterVersion)

    val properties = mutable.ListBuffer(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name")
    if (tableReaderVersion < readerVersion) {
      properties += DeltaConfigs.MIN_READER_VERSION.key -> readerVersion.toString
    }
    if (tableWriterVersion < writerVersion) {
      properties += DeltaConfigs.MIN_WRITER_VERSION.key -> writerVersion.toString
    }
    val propertiesStr = properties.map(kv => s"'${kv._1}' = '${kv._2}'").mkString(", ")
    sql(s"ALTER TABLE $deltaPath SET TBLPROPERTIES ($propertiesStr)")
  }

}

