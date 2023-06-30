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

package org.apache.spark.sql.delta.commands.convert

import org.apache.spark.sql.delta.{DeltaErrors, SerializableFileStatus}
import org.apache.spark.sql.delta.util.{DeltaFileOperations, PartitionUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.streaming.MetadataLogFileIndex
import org.apache.spark.util.SerializableConfiguration

/** A file manifest generated through recursively listing a base path. */
class ManualListingFileManifest(
    spark: SparkSession,
    override val basePath: String,
    serializableConf: SerializableConfiguration) extends ConvertTargetFileManifest {

  protected def doList(): Dataset[SerializableFileStatus] = {
    val conf = spark.sparkContext.broadcast(serializableConf)
    DeltaFileOperations
      .recursiveListDirs(
        spark, Seq(basePath), conf, hiddenDirNameFilter = ConvertUtils.hiddenDirNameFilter)
      .where("!isDir")
  }

  private lazy val list: Dataset[SerializableFileStatus] = {
    val ds = doList()
    ds.cache()
    ds
  }

  override lazy val allFiles: Dataset[ConvertTargetFile] = {
    import org.apache.spark.sql.delta.implicits._
    list.map(ConvertTargetFile(_))
  }

  override def close(): Unit = list.unpersist()
}

/** A file manifest generated through listing partition paths from Metastore catalog. */
class CatalogFileManifest(
    spark: SparkSession,
    override val basePath: String,
    catalogTable: CatalogTable,
    serializableConf: SerializableConfiguration) extends ConvertTargetFileManifest {

  // List of partition directories and corresponding partition values.
  private lazy val partitionList = {
    if (catalogTable.partitionSchema.isEmpty) {
      // Not a partitioned table.
      Seq(basePath -> Map.empty[String, String])
    } else {
      val partitions = spark.sessionState.catalog.listPartitions(catalogTable.identifier)
      partitions.map { partition =>
        val partitionDir = partition.storage.locationUri.map(_.toString())
          .getOrElse {
            val partitionDir =
              PartitionUtils.getPathFragment(partition.spec, catalogTable.partitionSchema)
            basePath.stripSuffix("/") + "/" + partitionDir
          }
        partitionDir -> partition.spec
      }
    }
  }

  override lazy val allFiles: Dataset[ConvertTargetFile] = {
    import org.apache.spark.sql.delta.implicits._
    if (partitionList.isEmpty) {
      throw DeltaErrors.convertToDeltaNoPartitionFound(catalogTable.identifier.unquotedString)
    }

    // Avoid the serialization of this CatalogFileManifest during distributed execution.
    val conf = spark.sparkContext.broadcast(serializableConf)
    val parallelism = spark.sessionState.conf.parallelPartitionDiscoveryParallelism
    val rdd = spark.sparkContext.parallelize(partitionList)
      .repartition(math.min(parallelism, partitionList.length))
      .mapPartitions { partitions =>
        partitions.flatMap ( partition =>
          DeltaFileOperations
            .localListDirs(conf.value.value, Seq(partition._1), recursive = false).filter(!_.isDir)
            .map(ConvertTargetFile(_, Some(partition._2)))
        )
      }
    spark.createDataset(rdd).cache()
  }

  override def close(): Unit = allFiles.unpersist()
}

/** A file manifest generated from pre-existing parquet MetadataLog. */
class MetadataLogFileManifest(
    spark: SparkSession,
    override val basePath: String) extends ConvertTargetFileManifest {

  val index = new MetadataLogFileIndex(spark, new Path(basePath), Map.empty, None)

  override lazy val allFiles: Dataset[ConvertTargetFile] = {
    import org.apache.spark.sql.delta.implicits._

    val rdd = spark.sparkContext.parallelize(index.allFiles).mapPartitions { _
      .map(SerializableFileStatus.fromStatus)
      .map(ConvertTargetFile(_))
    }
    val ds = spark.createDataset(rdd)
    ds.cache()
  }

  override def close(): Unit = allFiles.unpersist()
}
