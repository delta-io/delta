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

import org.apache.spark.sql.delta.RowIndexFilterType
import org.apache.spark.sql.delta.DeltaParquetFileFormat.DeletionVectorDescriptorWithFilterType
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
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

import org.apache.spark.sql.{DataFrame, Dataset, QueryTest}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.{SerializableConfiguration, Utils}

class DeltaParquetFileFormatSuite extends QueryTest
  with SharedSparkSession with DeltaSQLCommandTest {
  import testImplicits._

  // Read with deletion vectors has separate code paths based on vectorized Parquet
  // reader is enabled or not. Test both the combinations
  for {
    readIsRowDeletedCol <- BOOLEAN_DOMAIN
    readRowIndexCol <- BOOLEAN_DOMAIN
    rowIndexFilterType <- Seq(RowIndexFilterType.IF_CONTAINED, RowIndexFilterType.IF_NOT_CONTAINED)
    // this isn't need to be tested as it is same as regular reading path without DVs.
    if readIsRowDeletedCol || readRowIndexCol
  } {
    testWithBothParquetReaders(
      "read DV metadata columns: " +
        s"with isRowDeletedCol=$readIsRowDeletedCol, " +
        s"with rowIndexCol=$readRowIndexCol, " +
        s"with rowIndexFilterType=$rowIndexFilterType") {
      withTempDir { tempDir =>
        val tablePath = tempDir.toString

        // Generate a table with one parquet file containing multiple row groups.
        generateData(tablePath)

        val deltaLog = DeltaLog.forTable(spark, tempDir)
        val metadata = deltaLog.snapshot.metadata

        // Add additional field that has the deleted row flag to existing data schema
        var readingSchema = metadata.schema
        if (readIsRowDeletedCol) {
          readingSchema = readingSchema.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD)
        }
        if (readRowIndexCol) {
          readingSchema = readingSchema.add(DeltaParquetFileFormat.ROW_INDEX_STRUCT_FILED)
        }

        // Fetch the only file in the DeltaLog snapshot
        val addFile = deltaLog.snapshot.allFiles.collect()(0)
        val addFilePath = new Path(tempDir.toString, addFile.path)
        assertParquetHasMultipleRowGroups(addFilePath)

        val dv = generateDV(tablePath, 0, 200, 300, 756, 10352, 19999)

        val fs = addFilePath.getFileSystem(hadoopConf)
        val broadcastDvMap = spark.sparkContext.broadcast(
          Map(fs.getFileStatus(addFilePath).getPath().toUri ->
            DeletionVectorDescriptorWithFilterType(dv, rowIndexFilterType))
        )

        val broadcastHadoopConf = spark.sparkContext.broadcast(
          new SerializableConfiguration(hadoopConf))

        val deltaParquetFormat = new DeltaParquetFileFormat(
          deltaLog.snapshot.protocol,
          metadata,
          isSplittable = false,
          disablePushDowns = true,
          Some(tablePath),
          if (readIsRowDeletedCol) Some(broadcastDvMap) else None,
          if (readIsRowDeletedCol) Some(broadcastHadoopConf) else None)

        val fileIndex = DeltaLogFileIndex(deltaParquetFormat, fs, addFilePath :: Nil)

        val relation = HadoopFsRelation(
          fileIndex,
          fileIndex.partitionSchema,
          readingSchema,
          bucketSpec = None,
          deltaParquetFormat,
          options = Map.empty)(spark)
        val plan = LogicalRelation(relation)

        if (readIsRowDeletedCol) {
          // Select some rows that are deleted and some rows not deleted
          // Deleted row `value`: 0, 200, 300, 756, 10352, 19999
          // Not deleted row `value`: 7, 900
          val (deletedColumnValue, notDeletedColumnValue) = rowIndexFilterType match {
            case RowIndexFilterType.IF_CONTAINED => (1, 0)
            case RowIndexFilterType.IF_NOT_CONTAINED => (0, 1)
            case _ => (-1, -1) // Invalid, expecting the test to fail.
          }
          checkDatasetUnorderly(
            Dataset.ofRows(spark, plan)
              .filter("value in (0, 7, 200, 300, 756, 900, 10352, 19999)")
              .select("value", DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME)
              .as[(Int, Int)],
            (0, deletedColumnValue),
            (7, notDeletedColumnValue),
            (200, deletedColumnValue),
            (300, deletedColumnValue),
            (756, deletedColumnValue),
            (900, notDeletedColumnValue),
            (10352, deletedColumnValue),
            (19999, deletedColumnValue))
        }

        if (readRowIndexCol) {
          def rowIndexes(df: DataFrame): Set[Long] = {
            val colIndex = if (readIsRowDeletedCol) 2 else 1
            df.collect().map(_.getLong(colIndex)).toSet
          }

          val df = Dataset.ofRows(spark, plan)
          assert(rowIndexes(df) === Seq.range(0, 20000).toSet)

          assert(
            rowIndexes(
              df.filter("value in (0, 7, 200, 300, 756, 900, 10352, 19999)")) ===
            Seq(0, 7, 200, 300, 756, 900, 10352, 19999).toSet)
        }
      }
    }
  }

  /** Helper method to run the test with vectorized and non-vectorized Parquet readers */
  private def testWithBothParquetReaders(name: String)(f: => Any): Unit = {
    for (enableVectorizedParquetReader <- BOOLEAN_DOMAIN) {
      withSQLConf(
        "spark.sql.parquet.enableVectorizedReader" -> enableVectorizedParquetReader.toString) {
        test(s"$name, with vectorized Parquet reader=$enableVectorizedParquetReader)") {
          f
        }
      }
    }
  }

  /** Helper method to generate a table with single Parquet file with multiple rowgroups */
  private def generateData(tablePath: String): Unit = {
    // This is to generate a Parquet file with two row groups
    hadoopConf().set("parquet.block.size", (1024 * 50).toString)

    // Keep the number of partitions to 1 to generate a single Parquet data file
    val df = Seq.range(0, 20000).toDF().repartition(1)
    df.write.format("delta").mode("append").save(tablePath)

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
