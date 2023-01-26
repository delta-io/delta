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

import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore.pathToString
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.PathWithFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader

import org.apache.spark.sql.{Dataset, QueryTest}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.{SerializableConfiguration, Utils}

class DeltaParquetFileFormatSuite extends QueryTest
  with SharedSparkSession with DeltaSQLCommandTest {
  import testImplicits._

  // Read with deletion vectors has separate code paths based on vectorized Parquet
  // reader is enabled or not. Test both the combinations
  for (enableVectorizedParquetReader <- Seq("true", "false")) {
    test(
      s"read with DVs (vectorized Parquet reader enabled=$enableVectorizedParquetReader)") {
      withTempDir { tempDir =>
        spark.conf.set("spark.sql.parquet.enableVectorizedReader", enableVectorizedParquetReader)

        val tablePath = tempDir.toString

        // Generate a table with one parquet file containing multiple row groups.
        generateData(tablePath)

        val deltaLog = DeltaLog.forTable(spark, tempDir)
        val metadata = deltaLog.snapshot.metadata

        // Add additional field that has the deleted row flag to existing data schema
        val readingSchema = metadata.schema.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD)

        val addFilePath = new Path(
          tempDir.toString,
          deltaLog.snapshot.allFiles.collect()(0).path)
        assertParquetHasMultipleRowGroups(addFilePath)

        val dv = generateDV(tablePath, 0, 200, 300, 756, 10352, 19999)

        val fs = addFilePath.getFileSystem(hadoopConf)
        val broadcastDvMap = spark.sparkContext.broadcast(
          Map(fs.getFileStatus(addFilePath).getPath().toUri -> dv)
        )

        val broadcastHadoopConf = spark.sparkContext.broadcast(
          new SerializableConfiguration(hadoopConf))

        val deltaParquetFormat = new DeltaParquetFileFormat(
          metadata,
          isSplittable = false,
          disablePushDowns = true,
          Some(tablePath),
          Some(broadcastDvMap),
          Some(broadcastHadoopConf))

        val fileIndex = DeltaLogFileIndex(deltaParquetFormat, fs, addFilePath :: Nil)

        val relation = HadoopFsRelation(
          fileIndex,
          fileIndex.partitionSchema,
          readingSchema,
          bucketSpec = None,
          deltaParquetFormat,
          options = Map.empty)(spark)
        val plan = LogicalRelation(relation)

        // Select some rows that are deleted and some rows not deleted
        // Deleted row `value`: 0, 200, 300, 756, 10352, 19999
        // Not deleted row `value`: 7, 900
        checkDatasetUnorderly(
          Dataset.ofRows(spark, plan)
            .filter("value in (0, 7, 200, 300, 756, 900, 10352, 19999)")
            .as[(Int, Int)],
          (0, 1), (7, 0), (200, 1), (300, 1), (756, 1), (900, 0), (10352, 1), (19999, 1)
        )
      }
    }
  }

  /** Helper method to generate a table with single Parquet file with multiple rowgroups */
  private def generateData(tablePath: String): Unit = {
    // This is to generate a Parquet file with two row groups
    hadoopConf().set("parquet.block.size", (1024 * 50).toString)

    // Keep the number of partitions to 1 to generate a single Parquet data file
    val df = Seq.range(0, 20000).toDF().repartition(1)
    df.write
        .format("delta").mode("append").save(tablePath)

    // Set DFS block size to be less than Parquet rowgroup size, to allow
    // the file split logic to kick-in, but gets turned off due to the
    // disabling of file splitting in DeltaParquetFileFormat when DVs are present.
    hadoopConf().set("dfs.block.size", (1024 * 20).toString)
  }

  /** Utility method that generates deletion vector based on the given row indexes */
  private def generateDV(tablePath: String, rowIndexes: Long *): DeletionVectorDescriptor = {
    val bitmap = RoaringBitmapArray(rowIndexes: _*)
    val tableWithFS = PathWithFileSystem.withConf(new Path(tablePath), hadoopConf)
    val dvPath = dvStore.generateUniqueNameInTable(tableWithFS)
    val serializedBitmap = bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
    val dvRange = Utils.tryWithResource(dvStore.createWriter(dvPath)) { writer =>
      writer.write(serializedBitmap)
    }
    DeletionVectorDescriptor.onDiskWithAbsolutePath(
      pathToString(dvPath.makeQualified().path),
      dvRange.length,
      rowIndexes.size,
      Some(dvRange.offset))
  }

  private def assertParquetHasMultipleRowGroups(filePath: Path): Unit = {
    val parquetMetadata = ParquetFileReader.readFooter(
      hadoopConf,
      filePath,
      ParquetMetadataConverter.NO_FILTER)
    assert(parquetMetadata.getBlocks.size() > 1)
  }

  private def hadoopConf(): Configuration = {
    // scalastyle:off hadoopconfiguration
    // This is to generate a Parquet file with two row groups
    spark.sparkContext.hadoopConfiguration
    // scalastyle:on hadoopconfiguration
  }

  lazy val dvStore: DeletionVectorStore = DeletionVectorStore.createInstance(hadoopConf)
}
