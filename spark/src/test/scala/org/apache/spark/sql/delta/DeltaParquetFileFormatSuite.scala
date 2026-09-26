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

import org.apache.spark.sql.delta.{RowIndexFilter, RowIndexFilterProvider, RowIndexFilterType}
import org.apache.spark.sql.delta.DataFrameUtils
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.deletionvectors.{DropAllRowsFilter, DropMarkedRowsFilter, RoaringBitmapArray}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader

import org.apache.spark.sql.{DataFrame, Dataset, QueryTest, Row}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructType}

trait DeltaParquetFileFormatSuiteBase
    extends QueryTest
    with SharedSparkSession
    with DeletionVectorsTestUtils
    with DeltaSQLCommandTest {
  import testImplicits._

  /** Helper method to run the test with vectorized and non-vectorized Parquet readers */
  protected def testWithBothParquetReaders(name: String)(f: => Any): Unit = {
    for {
      enableVectorizedParquetReader <- BOOLEAN_DOMAIN
      readColumnarBatchAsRows <- BOOLEAN_DOMAIN
      // don't run for the combination (vectorizedReader=false, readColumnarBathAsRows = false)
      // as the non-vectorized reader always generates and returns rows, unlike the vectorized
      // reader which internally generates columnar batches but can returns either columnar batches
      // or rows from the columnar batch depending upon the config.
      if enableVectorizedParquetReader || readColumnarBatchAsRows
    } {
      test(s"$name, with vectorized Parquet reader=$enableVectorizedParquetReader, " +
        s"with readColumnarBatchAsRows=$readColumnarBatchAsRows") {
        // Set the max code gen fields to 0 to force the vectorized Parquet reader generate rows
        // from columnar batches.
        val codeGenMaxFields = if (readColumnarBatchAsRows) "0" else "100"
        withSQLConf(
          "spark.sql.parquet.enableVectorizedReader" -> enableVectorizedParquetReader.toString,
          "spark.sql.codegen.maxFields" -> codeGenMaxFields) {
          f
        }
      }
    }
  }

  /** Helper method to generate a table with single Parquet file with multiple rowgroups */
  protected def generateData(tablePath: String): Unit = {
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

  protected def assertParquetHasMultipleRowGroups(filePath: Path): Unit = {
    val parquetMetadata = ParquetFileReader.readFooter(
      hadoopConf,
      filePath,
      ParquetMetadataConverter.NO_FILTER)
    assert(parquetMetadata.getBlocks.size() > 1)
  }

  protected def hadoopConf(): Configuration = {
    // scalastyle:off hadoopconfiguration
    // This is to generate a Parquet file with two row groups
    spark.sparkContext.hadoopConfiguration
    // scalastyle:on hadoopconfiguration
  }

  lazy val dvStore: DeletionVectorStore = DeletionVectorStore.createInstance(hadoopConf)
}


class DeltaParquetFileFormatSuite extends DeltaParquetFileFormatSuiteBase {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key, "false")
  }

  // Read with deletion vectors has separate code paths based on vectorized Parquet
  // reader is enabled or not. Test both the combinations
  for {
    readIsRowDeletedCol <- BOOLEAN_DOMAIN
    readRowIndexCol <- BOOLEAN_DOMAIN
    enableDVs <- BOOLEAN_DOMAIN
    if (enableDVs && readIsRowDeletedCol) || !enableDVs
  } {
    testWithBothParquetReaders(
      s"isDeletionVectorsEnabled=$enableDVs, read DV metadata columns: " +
        s"with isRowDeletedCol=$readIsRowDeletedCol, " +
        s"with rowIndexCol=$readRowIndexCol") {
      withSQLConf(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey ->
          enableDVs.toString) {
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
            readingSchema = readingSchema.add(DeltaParquetFileFormat.ROW_INDEX_STRUCT_FIELD)
          }

          // Fetch the only file in the DeltaLog snapshot
          val addFile = deltaLog.snapshot.allFiles.collect()(0)

          if (enableDVs) {
            removeRowsFromFile(deltaLog, addFile, Seq(0, 200, 300, 756, 10352, 19999))
          }

          val addFilePath = addFile.absolutePath(deltaLog)
          assertParquetHasMultipleRowGroups(addFilePath)

          val deltaParquetFormat = new DeltaParquetFileFormat(
            deltaLog.snapshot.protocol,
            metadata,
            nullableRowTrackingConstantFields = false,
            nullableRowTrackingGeneratedFields = false,
            optimizationsEnabled = false,
            if (enableDVs) Some(tablePath) else None)

          val fileIndex = TahoeLogFileIndex(spark, deltaLog)

          val relation = HadoopFsRelation(
            fileIndex,
            fileIndex.partitionSchema,
            readingSchema,
            bucketSpec = None,
            deltaParquetFormat,
            options = Map.empty)(spark)
          val plan = LogicalRelation(relation)

          if (readIsRowDeletedCol) {
            val (deletedColumnValue, notDeletedColumnValue) = (1, 0)
            if (enableDVs) {
              // Select some rows that are deleted and some rows not deleted
              // Deleted row `value`: 0, 200, 300, 756, 10352, 19999
              // Not deleted row `value`: 7, 900
              checkDatasetUnorderly(
                DataFrameUtils.ofRows(spark, plan)
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
            } else {
              checkDatasetUnorderly(
                DataFrameUtils.ofRows(spark, plan)
                  .filter("value in (0, 7, 200, 300, 756, 900, 10352, 19999)")
                  .select("value", DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME)
                  .as[(Int, Int)],
                (0, notDeletedColumnValue),
                (7, notDeletedColumnValue),
                (200, notDeletedColumnValue),
                (300, notDeletedColumnValue),
                (756, notDeletedColumnValue),
                (900, notDeletedColumnValue),
                (10352, notDeletedColumnValue),
                (19999, notDeletedColumnValue))
            }
          }

          if (readRowIndexCol) {
            def rowIndexes(df: DataFrame): Set[Long] = {
              val colIndex = if (readIsRowDeletedCol) 2 else 1
              df.collect().map(_.getLong(colIndex)).toSet
            }

            val df = DataFrameUtils.ofRows(spark, plan)
            assert(rowIndexes(df) === Seq.range(0, 20000).toSet)

            assert(
              rowIndexes(
                df.filter("value in (0, 7, 200, 300, 756, 900, 10352, 19999)")) ===
                Seq(0, 7, 200, 300, 756, 900, 10352, 19999).toSet)
          }
        }
      }
    }
  }
}

/** Exercises the portable provider marker path even when native filtering is also available. */
class DeltaParquetFileFormatProviderSuite extends QueryTest with SharedSparkSession {
  import DeltaParquetFileFormat._
  import DeltaParquetFileFormatProviderSuite.{DropAllProvider, MarkedRowsProvider, PortableFileFormat}


  private def withParquetFiles(f: (Path, Array[FileStatus]) => Unit): Unit = {
    withTempDir { dir =>
      val root = new Path(dir.toString)
      val files = (0 until 2).map { file =>
        val path = new Path(root, s"leaf $file")
        spark.range(file * 8L, (file + 1) * 8L).coalesce(1).write.parquet(path.toString)
        path.getFileSystem(spark.sessionState.newHadoopConf()).listStatus(path)
          .find(_.getPath.getName.endsWith(".parquet")).get
      }.toArray
      f(root, files)
    }
  }

  private def readMarkers(
      root: Path,
      files: Array[FileStatus],
      metadata: Map[String, Map[String, Any]],
      useMetadataRowIndex: Boolean): DataFrame = {
    val format = new PortableFileFormat(root, useMetadataRowIndex)
    // Attach only portable metadata, so this exercises the same reader wrapper in both engines.
    val index = new DeltaLogFileIndex(format, files, perFileMetadata = metadata)
    val schema = new StructType().add("id", LongType).add(IS_ROW_DELETED_STRUCT_FIELD)
    val readSchema = if (useMetadataRowIndex) schema else schema.add(ROW_INDEX_STRUCT_FIELD)
    val relation = HadoopFsRelation(
      index, index.partitionSchema, readSchema, None, format, Map.empty)(spark)
    val plan = LogicalRelation(relation)
    val withMetadata = plan.copy(output = plan.output :+ format.createFileMetadataCol())
    val rowIndex = if (useMetadataRowIndex) "_metadata.row_index" else ROW_INDEX_COLUMN_NAME
    DataFrameUtils.ofRows(spark, withMetadata)
      .select(col("id"), col(IS_ROW_DELETED_COLUMN_NAME), col(rowIndex))
  }

  for {
    useMetadataRowIndex <- BOOLEAN_DOMAIN
    (vectorized, codegenFields) <- Seq((true, 100), (true, 0), (false, 0))
  } {
    test(s"provider markers preserve all rows: metadataIndex=$useMetadataRowIndex, " +
        s"vectorized=$vectorized, codegenFields=$codegenFields") {
      withSQLConf(
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorized.toString,
        SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE.key -> "2",
        SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> codegenFields.toString) {
        withParquetFiles { (root, files) =>
          val metadata = Map(files.head.getPath.toString ->
            Map[String, Any](FILE_ROW_INDEX_FILTER_PROVIDER -> MarkedRowsProvider))
          val rows = readMarkers(root, files, metadata, useMetadataRowIndex)
          checkAnswer(rows, (0L until 16L).map { id =>
            val marker = if (id == 1 || id == 5) RowIndexFilter.DROP_ROW_VALUE
              else RowIndexFilter.KEEP_ROW_VALUE
            Row(id, marker, id % 8)
          })
        }
      }
    }
  }

  test("a descriptor-free provider is loaded on the executor") {
    withParquetFiles { (root, files) =>
      val metadata = Map(files.head.getPath.toString ->
        Map[String, Any](FILE_ROW_INDEX_FILTER_PROVIDER -> DropAllProvider))
      checkAnswer(readMarkers(root, files, metadata, useMetadataRowIndex = false),
        (0L until 16L).map { id =>
          Row(id, if (id < 8) RowIndexFilter.DROP_ROW_VALUE else RowIndexFilter.KEEP_ROW_VALUE,
            id % 8)
        })
    }
  }

  test("a provider cannot be combined with encoded deletion vector metadata") {
    withParquetFiles { (root, files) =>
      for (extra <- Seq(
        Map[String, Any](FILE_ROW_INDEX_FILTER_ID_ENCODED -> "unused"),
        Map[String, Any](FILE_ROW_INDEX_FILTER_TYPE -> RowIndexFilterType.IF_CONTAINED))) {
        val metadata = Map(files.head.getPath.toString ->
          (extra + (FILE_ROW_INDEX_FILTER_PROVIDER -> DropAllProvider)))
        val error = intercept[Exception] {
          readMarkers(root, files, metadata, useMetadataRowIndex = false).collect()
        }
        val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
        assert(causes.exists(e => Option(e.getMessage).exists(
          _.contains("A row index filter provider cannot be combined"))))
      }
    }
  }

  test("a log file index rejects simultaneous provider and metadata maps") {
    val error = intercept[IllegalArgumentException] {
      new DeltaLogFileIndex(
        DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET,
        Array.empty[FileStatus],
        perFileRowIndexFilters = Map("leaf" -> DropAllProvider),
        perFileMetadata = Map("leaf" -> Map(FILE_ROW_INDEX_FILTER_TYPE ->
          RowIndexFilterType.IF_CONTAINED)))
    }
    assert(error.getMessage.contains("perFileRowIndexFilters and perFileMetadata"))
  }
}

object DeltaParquetFileFormatProviderSuite {
  /** Uses the shared portable reader without the native V1 reader wrapper. */
  class PortableFileFormat(root: Path, useMetadataRowIndex: Boolean)
    extends DeltaParquetFileFormatBase(
      ProtocolMetadataAdapterV1(
        Protocol().withFeatures(Set(DeletionVectorsTableFeature)), Metadata()),
      optimizationsEnabled = false,
      tablePath = Some(root.toString),
      useMetadataRowIndexOpt = Some(useMetadataRowIndex))

  /** Only the portable retrieval contract may be used by this reader path. */
  abstract class PortableProvider extends RowIndexFilterProvider {
  }

  case object MarkedRowsProvider extends PortableProvider {
    override def retrieve(conf: Configuration): RowIndexFilter =
      new DropMarkedRowsFilter(RoaringBitmapArray(1, 5))
  }

  /** No bitmap or descriptor is needed to implement this provider. */
  case object DropAllProvider extends PortableProvider {
    override def retrieve(conf: Configuration): RowIndexFilter = DropAllRowsFilter
  }
}

class DeltaParquetFileFormatWithPredicatePushdownSuite extends DeltaParquetFileFormatSuiteBase {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectorsForAllSupportedOperations(spark)
    spark.conf.set(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key, "true")
  }

  for {
    rowIndexFilterType <- Seq(RowIndexFilterType.IF_CONTAINED, RowIndexFilterType.IF_NOT_CONTAINED)
  } testWithBothParquetReaders("read DV metadata columns: " +
      s"with rowIndexFilterType=$rowIndexFilterType") {
    withTempDir { tempDir =>
      val tablePath = tempDir.toString

      // Generate a table with one parquet file containing multiple row groups.
      generateData(tablePath)

      val deltaLog = DeltaLog.forTable(spark, tempDir)
      val metadata = deltaLog.update().metadata

      // Add additional field that has the deleted row flag to existing data schema
      val readingSchema = metadata.schema.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD)

      // Fetch the only file in the DeltaLog snapshot
      val addFile = deltaLog.update().allFiles.collect()(0)
      removeRowsFromFile(deltaLog, addFile, Seq(0, 200, 300, 756, 10352, 19999))

      val addFilePath = addFile.absolutePath(deltaLog)
      assertParquetHasMultipleRowGroups(addFilePath)

      val deltaParquetFormat = new DeltaParquetFileFormat(
        deltaLog.update().protocol,
        metadata,
        nullableRowTrackingConstantFields = false,
        nullableRowTrackingGeneratedFields = false,
        optimizationsEnabled = true,
        Some(tablePath))

      val fileIndex = TahoeLogFileIndex(spark, deltaLog)

      val relation = HadoopFsRelation(
        fileIndex,
        fileIndex.partitionSchema,
        readingSchema,
        bucketSpec = None,
        deltaParquetFormat,
        options = Map.empty)(spark)

      val plan = LogicalRelation(relation)
      val planWithMetadataCol =
        plan.copy(output = plan.output :+ deltaParquetFormat.createFileMetadataCol())
      val (deletedColumnValue, notDeletedColumnValue) = (1, 0)

      // Select some rows that are deleted and some rows not deleted
      // Deleted row `value`: 0, 200, 300, 756, 10352, 19999
      // Not deleted row `value`: 7, 900
      checkDatasetUnorderly(
        DataFrameUtils.ofRows(spark, planWithMetadataCol)
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
  }
}
