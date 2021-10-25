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

import org.apache.spark.sql.delta.schema.SchemaUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{AtomicType, StructField, StructType}

trait DeltaColumnMappingTestUtilsBase extends SharedSparkSession {

  private val PHYSICAL_NAME_REGEX =
    "col-[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}".r

  def columnMappingEnabled: Boolean = {
     val columnMappingMode = spark.conf.getOption(
      DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey
    ).getOrElse("none")
    columnMappingMode != "none"
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
  protected def assertPartitionExists(
      partCol: String,
      partValue: String,
      deltaLog: DeltaLog): Unit = {
    if (columnMappingEnabled) {
      // column mapping always use random file prefixes so we can't compare path
      assert(getPartitionedFilePathsWithDeltaLog(partCol, partValue, deltaLog).nonEmpty)
    } else {
      val partEscaped = s"${ExternalCatalogUtils.escapePathName(partCol)}=$partValue"
      val partFile = new File(new File(deltaLog.dataPath.toUri.getPath), partEscaped)
      assert(partFile.listFiles().nonEmpty)
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
   * Get partition file paths
   * @param partCol Logical or physical partition name
   * @param partValue Partition value
   * @param deltaLog DeltaLog
   * @return List of paths
   */
  protected def getPartitionedFilePathsWithDeltaLog(
      partCol: String,
      partValue: String,
      deltaLog: DeltaLog): Array[String] = {
    val colName = PHYSICAL_NAME_REGEX
      .findFirstIn(partCol)
      .getOrElse(getPhysicalName(partCol, deltaLog))

    deltaLog.update().allFiles.collect()
      .filter(_.partitionValues(colName) == partValue).map(_.path)
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
    configs.filter(p =>
      !Seq(
        DeltaConfigs.COLUMN_MAPPING_MAX_ID.key,
        DeltaConfigs.COLUMN_MAPPING_MODE.key
      ).contains(p._1)
    )
  }

  /**
   * Convert (nested) column name string into physical name with reference from DeltaLog
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

}

trait DeltaColumnMappingTestUtils extends DeltaColumnMappingTestUtilsBase


/**
 * Include this trait to enable Id column mapping mode for a suite
 */
trait DeltaColumnMappingEnableIdMode extends SharedSparkSession {
  override def sparkConf: SparkConf =
    super.sparkConf.set(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey, "id")
}

/**
 * Include this trait to enable Name column mapping mode for a suite
 */
trait DeltaColumnMappingEnableNameMode extends SharedSparkSession {
  override def sparkConf: SparkConf =
    super.sparkConf.set(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey, "name")
}
