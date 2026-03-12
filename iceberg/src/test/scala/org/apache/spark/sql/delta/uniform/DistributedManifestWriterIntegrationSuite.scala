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

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{DeltaFileProviderUtils, DeltaLog}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.icebergShaded.{
  DistributedManifestWriter,
  IcebergTransactionUtils,
  ManifestWriteContext
}

import org.apache.spark.sql.{Dataset, QueryTest}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import org.apache.hadoop.fs.Path
import shadedForDelta.org.apache.iceberg.{
  DataFile,
  GenericManifestFile,
  ManifestFile,
  ManifestFiles,
  ManifestWriter,
  PartitionSpec,
  Schema => IcebergSchema
}
import shadedForDelta.org.apache.iceberg.hadoop.HadoopFileIO
import shadedForDelta.org.apache.iceberg.types.Types.{
  DoubleType => IcebergDoubleType,
  IntegerType => IcebergIntType,
  NestedField,
  StringType => IcebergStringType
}

/**
 * Integration tests that exercise the full DistributedManifestWriter pipeline
 * with real Delta tables and verify that manifest .avro files are written to disk
 * with correct content. These tests also compare the distributed path output against
 * an equivalent driver-based path to ensure both produce identical results.
 */
class DistributedManifestWriterIntegrationSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  /**
   * Simulate the driver-based manifest writing path: collect all AddFiles to the driver,
   * convert each to an Iceberg DataFile, write them into a single manifest file.
   * Returns (ManifestFile, Seq[DataFile-metadata]).
   */
  /**
   * Parameters for the driver-based manifest writing path.
   */
  private case class DriverWriteParams(
      statsParser: String => org.apache.spark.sql.catalyst.InternalRow,
      tableSchema: StructType,
      tableVersion: Long,
      statsSchema: StructType,
      metadataOutputDir: String,
      formatVersion: Int,
      hadoopConf: org.apache.hadoop.conf.Configuration)

  // scalastyle:off argcount
  private def writeManifestViaDriverPath(
      addFiles: Seq[AddFile],
      tablePath: Path,
      partitionSpec: PartitionSpec,
      logicalToPhysical: Map[String, String],
      params: DriverWriteParams): (ManifestFile, Seq[DataFile]) = {
  // scalastyle:on argcount

    val fileIO = new HadoopFileIO(params.hadoopConf)
    val manifestPath = s"${params.metadataOutputDir}/driver-m-${UUID.randomUUID()}.avro"
    val outputFile = fileIO.newOutputFile(manifestPath)
    val writer: ManifestWriter[DataFile] =
      ManifestFiles.write(params.formatVersion, partitionSpec, outputFile, null)

    val dataFiles = addFiles.map { addFile =>
      IcebergTransactionUtils.convertDeltaAddFileToIcebergDataFile(
        addFile, tablePath, partitionSpec, logicalToPhysical,
        params.statsParser, params.tableSchema, params.tableVersion, params.statsSchema)
    }

    dataFiles.foreach(writer.add)
    writer.close()

    (writer.toManifestFile, dataFiles)
  }

  /**
   * Read DataFile entries back from a manifest file on disk.
   * Manifests written with snapshotId=null (as in production) need a non-null
   * snapshot ID to be read back, so we create a copy with a synthetic ID.
   */
  private def readDataFilesFromManifest(
      manifest: ManifestFile,
      partitionSpec: PartitionSpec,
      hadoopConf: org.apache.hadoop.conf.Configuration): Seq[DataFile] = {
    val fileIO = new HadoopFileIO(hadoopConf)
    val specsById: java.util.Map[Integer, PartitionSpec] =
      Map(Integer.valueOf(partitionSpec.specId()) -> partitionSpec).asJava
    // Manifests are written with snapshotId=null (Iceberg assigns at commit time).
    // For test reading, assign a synthetic snapshot ID.
    val readableManifest = GenericManifestFile.copyOf(manifest)
      .withSnapshotId(java.lang.Long.valueOf(1L)).build()
    ManifestFiles.read(readableManifest, fileIO, specsById).asScala.toSeq
  }

  test("distributed writer produces manifest files on disk with correct DataFile entries") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      // Create a Delta table with multiple files
      spark.range(0, 100, 1, 5).toDF("id")
        .write.format("delta").save(tablePath)

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      val snapshot = deltaLog.update()
      assert(snapshot.numOfFiles >= 5, s"Expected >= 5 files, got ${snapshot.numOfFiles}")

      val allFiles: Dataset[AddFile] = snapshot.allFiles

      // Build the ManifestWriteContext
      // scalastyle:off deltahadoopconfiguration
      val hadoopConf = spark.sessionState.newHadoopConf()
      // scalastyle:on deltahadoopconfiguration
      val hadoopConfBroadcast = spark.sparkContext.broadcast(
        new SerializableConfiguration(hadoopConf))

      val metadataDir = s"$tablePath/metadata"
      new java.io.File(metadataDir).mkdirs()

      val partitionSpec = PartitionSpec.unpartitioned()
      val ctx = ManifestWriteContext(
        tablePath = tablePath,
        partitionSpec = partitionSpec,
        logicalToPhysicalPartitionNames = Map.empty,
        statsSchema = snapshot.statsSchema,
        tableSchema = snapshot.schema,
        sessionLocalTimeZone = spark.sessionState.conf.sessionLocalTimeZone,
        tableVersion = snapshot.version,
        metadataOutputLocation = metadataDir,
        formatVersion = 2,
        targetManifestSizeBytes = DistributedManifestWriter.DEFAULT_TARGET_MANIFEST_SIZE_BYTES,
        hadoopConfBroadcast = hadoopConfBroadcast
      )

      // Execute the distributed writer
      val manifests = DistributedManifestWriter.writeManifestsDistributed(
        spark, allFiles, ctx, numPartitions = 3)

      // Verify manifests were written
      assert(manifests.nonEmpty, "Should produce at least one manifest file")

      // Verify manifest .avro files exist on disk
      manifests.foreach { m =>
        val manifestFile = new java.io.File(m.path().replaceFirst("file:", ""))
        assert(manifestFile.exists(), s"Manifest file should exist on disk: ${m.path()}")
        assert(manifestFile.length() > 0, s"Manifest file should not be empty: ${m.path()}")
      }

      // Read back all DataFile entries from all manifests
      val distributedDataFiles = manifests.flatMap { m =>
        readDataFilesFromManifest(m, partitionSpec, hadoopConf)
      }

      // Should have one DataFile per AddFile in the snapshot
      assert(distributedDataFiles.size === snapshot.numOfFiles.toInt,
        s"Expected ${snapshot.numOfFiles} DataFiles, got ${distributedDataFiles.size}")

      // Every DataFile path should point to a real parquet file
      distributedDataFiles.foreach { df =>
        assert(df.path().toString.endsWith(".parquet"),
          s"DataFile path should end with .parquet: ${df.path()}")
        assert(df.fileSizeInBytes() > 0, "DataFile size should be positive")
        assert(df.format().toString === "PARQUET")
      }

      // Verify total record count matches
      val totalRecords = distributedDataFiles.map(_.recordCount()).sum
      assert(totalRecords === 100L,
        s"Total record count should be 100, got $totalRecords")
    }
  }

  test("distributed and driver paths produce identical DataFile metadata") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      // Create a Delta table with several files
      spark.range(0, 50, 1, 4).toDF("id")
        .write.format("delta").save(tablePath)

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      val snapshot = deltaLog.update()
      val allFilesCollected = snapshot.allFiles.collect().toSeq
      val dataPath = new Path(tablePath)

      // scalastyle:off deltahadoopconfiguration
      val hadoopConf = spark.sessionState.newHadoopConf()
      // scalastyle:on deltahadoopconfiguration
      val hadoopConfBroadcast = spark.sparkContext.broadcast(
        new SerializableConfiguration(hadoopConf))

      val metadataDir = s"$tablePath/metadata"
      new java.io.File(metadataDir).mkdirs()

      val partitionSpec = PartitionSpec.unpartitioned()
      val statsParser = DeltaFileProviderUtils.createJsonStatsParser(
        snapshot.statsSchema, spark.sessionState.conf.sessionLocalTimeZone)

      // === Driver path ===
      val (driverManifest, driverDataFiles) = writeManifestViaDriverPath(
        allFilesCollected, dataPath, partitionSpec, Map.empty,
        DriverWriteParams(statsParser, snapshot.schema, snapshot.version,
          snapshot.statsSchema, metadataDir, 2, hadoopConf))

      // === Distributed path ===
      val ctx = ManifestWriteContext(
        tablePath = tablePath,
        partitionSpec = partitionSpec,
        logicalToPhysicalPartitionNames = Map.empty,
        statsSchema = snapshot.statsSchema,
        tableSchema = snapshot.schema,
        sessionLocalTimeZone = spark.sessionState.conf.sessionLocalTimeZone,
        tableVersion = snapshot.version,
        metadataOutputLocation = metadataDir,
        formatVersion = 2,
        targetManifestSizeBytes = DistributedManifestWriter.DEFAULT_TARGET_MANIFEST_SIZE_BYTES,
        hadoopConfBroadcast = hadoopConfBroadcast
      )

      val distributedManifests = DistributedManifestWriter.writeManifestsDistributed(
        spark, snapshot.allFiles, ctx, numPartitions = 2)

      // Read back DataFiles from both paths
      val driverResults = readDataFilesFromManifest(driverManifest, partitionSpec, hadoopConf)
      val distributedResults = distributedManifests.flatMap { m =>
        readDataFilesFromManifest(m, partitionSpec, hadoopConf)
      }

      // Same number of DataFiles
      assert(driverResults.size === distributedResults.size,
        s"File count mismatch: driver=${driverResults.size}, " +
          s"distributed=${distributedResults.size}")

      // Build maps keyed by file path for comparison
      val driverByPath = driverResults.map(df => df.path().toString -> df).toMap
      val distributedByPath = distributedResults.map(df => df.path().toString -> df).toMap

      // Same set of file paths
      assert(driverByPath.keySet === distributedByPath.keySet,
        "Driver and distributed paths should reference the same set of files")

      // Compare DataFile metadata field by field
      driverByPath.foreach { case (path, driverDf) =>
        val distDf = distributedByPath(path)
        assert(driverDf.fileSizeInBytes() === distDf.fileSizeInBytes(),
          s"Size mismatch for $path")
        assert(driverDf.recordCount() === distDf.recordCount(),
          s"Record count mismatch for $path")
        assert(driverDf.format() === distDf.format(),
          s"Format mismatch for $path")
        assert(driverDf.partition() === distDf.partition(),
          s"Partition mismatch for $path")
      }

      // Total record counts should match
      assert(driverResults.map(_.recordCount()).sum ===
        distributedResults.map(_.recordCount()).sum)
    }
  }

  test("distributed and driver paths produce identical results for partitioned table") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      // Create a partitioned Delta table
      spark.createDataFrame(Seq(
        (1, "A", 10.0), (2, "A", 20.0), (3, "B", 30.0),
        (4, "B", 40.0), (5, "C", 50.0), (6, "C", 60.0),
        (7, "A", 70.0), (8, "B", 80.0)
      )).toDF("id", "category", "value")
        .write.format("delta").partitionBy("category").save(tablePath)

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      val snapshot = deltaLog.update()
      val allFilesCollected = snapshot.allFiles.collect().toSeq
      val dataPath = new Path(tablePath)
      assert(snapshot.numOfFiles >= 3, "Should have files in multiple partitions")

      // scalastyle:off deltahadoopconfiguration
      val hadoopConf = spark.sessionState.newHadoopConf()
      // scalastyle:on deltahadoopconfiguration
      val hadoopConfBroadcast = spark.sparkContext.broadcast(
        new SerializableConfiguration(hadoopConf))

      val metadataDir = s"$tablePath/metadata"
      new java.io.File(metadataDir).mkdirs()

      // Build Iceberg schema and partition spec
      val icebergSchema = new IcebergSchema(
        NestedField.required(1, "id", IcebergIntType.get()),
        NestedField.required(2, "category", IcebergStringType.get()),
        NestedField.required(3, "value", IcebergDoubleType.get())
      )
      val partitionSpec = IcebergTransactionUtils.createPartitionSpec(
        icebergSchema, Seq("category"))
      val logicalToPhysical = IcebergTransactionUtils.getPartitionPhysicalNameMapping(
        snapshot.metadata.partitionSchema)

      val statsParser = DeltaFileProviderUtils.createJsonStatsParser(
        snapshot.statsSchema, spark.sessionState.conf.sessionLocalTimeZone)

      // === Driver path ===
      val (driverManifest, _) = writeManifestViaDriverPath(
        allFilesCollected, dataPath, partitionSpec, logicalToPhysical,
        DriverWriteParams(statsParser, snapshot.schema, snapshot.version,
          snapshot.statsSchema, metadataDir, 2, hadoopConf))

      // === Distributed path ===
      val ctx = ManifestWriteContext(
        tablePath = tablePath,
        partitionSpec = partitionSpec,
        logicalToPhysicalPartitionNames = logicalToPhysical,
        statsSchema = snapshot.statsSchema,
        tableSchema = snapshot.schema,
        sessionLocalTimeZone = spark.sessionState.conf.sessionLocalTimeZone,
        tableVersion = snapshot.version,
        metadataOutputLocation = metadataDir,
        formatVersion = 2,
        targetManifestSizeBytes = DistributedManifestWriter.DEFAULT_TARGET_MANIFEST_SIZE_BYTES,
        hadoopConfBroadcast = hadoopConfBroadcast
      )

      val distributedManifests = DistributedManifestWriter.writeManifestsDistributed(
        spark, snapshot.allFiles, ctx, numPartitions = 2)

      // Read back DataFiles from both paths
      val driverResults = readDataFilesFromManifest(driverManifest, partitionSpec, hadoopConf)
      val distributedResults = distributedManifests.flatMap { m =>
        readDataFilesFromManifest(m, partitionSpec, hadoopConf)
      }

      assert(driverResults.size === distributedResults.size,
        s"File count mismatch: driver=${driverResults.size}, " +
          s"distributed=${distributedResults.size}")

      val driverByPath = driverResults.map(df => df.path().toString -> df).toMap
      val distributedByPath = distributedResults.map(df => df.path().toString -> df).toMap

      assert(driverByPath.keySet === distributedByPath.keySet)

      // Compare partition values and file metadata
      driverByPath.foreach { case (path, driverDf) =>
        val distDf = distributedByPath(path)
        assert(driverDf.fileSizeInBytes() === distDf.fileSizeInBytes(),
          s"Size mismatch for $path")
        assert(driverDf.recordCount() === distDf.recordCount(),
          s"Record count mismatch for $path")

        // Compare partition values
        val driverPartition = driverDf.partition()
        val distPartition = distDf.partition()
        for (i <- 0 until driverPartition.size()) {
          assert(
            driverPartition.get(i, classOf[AnyRef]) ===
              distPartition.get(i, classOf[AnyRef]),
            s"Partition value mismatch at index $i for $path")
        }
      }

      // Verify partition values are present and correct
      val partitionValues = distributedResults.map { df =>
        df.partition().get(0, classOf[String])
      }.toSet
      assert(partitionValues === Set("A", "B", "C"),
        s"Expected partitions {A, B, C}, got $partitionValues")
    }
  }

  test("distributed writer handles empty dataset gracefully") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      // Create an empty Delta table
      spark.range(0).toDF("id")
        .write.format("delta").save(tablePath)

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      val snapshot = deltaLog.update()

      // scalastyle:off deltahadoopconfiguration
      val hadoopConf = spark.sessionState.newHadoopConf()
      // scalastyle:on deltahadoopconfiguration
      val hadoopConfBroadcast = spark.sparkContext.broadcast(
        new SerializableConfiguration(hadoopConf))

      val metadataDir = s"$tablePath/metadata"
      new java.io.File(metadataDir).mkdirs()

      val ctx = ManifestWriteContext(
        tablePath = tablePath,
        partitionSpec = PartitionSpec.unpartitioned(),
        logicalToPhysicalPartitionNames = Map.empty,
        statsSchema = snapshot.statsSchema,
        tableSchema = snapshot.schema,
        sessionLocalTimeZone = spark.sessionState.conf.sessionLocalTimeZone,
        tableVersion = snapshot.version,
        metadataOutputLocation = metadataDir,
        formatVersion = 2,
        targetManifestSizeBytes = DistributedManifestWriter.DEFAULT_TARGET_MANIFEST_SIZE_BYTES,
        hadoopConfBroadcast = hadoopConfBroadcast
      )

      val manifests = DistributedManifestWriter.writeManifestsDistributed(
        spark, snapshot.allFiles, ctx, numPartitions = 2)

      // Empty table should produce no manifests (empty partitions produce no output)
      assert(manifests.isEmpty, "Empty dataset should produce no manifest files")
    }
  }

  test("manifest rolling produces multiple manifests for large data") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      // Create a table with many small files to exercise manifest rolling
      for (i <- 0 until 20) {
        spark.range(i * 10, (i + 1) * 10).toDF("id")
          .write.format("delta").mode("append").save(tablePath)
      }

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      val snapshot = deltaLog.update()
      assert(snapshot.numOfFiles >= 20, s"Expected >= 20 files, got ${snapshot.numOfFiles}")

      // scalastyle:off deltahadoopconfiguration
      val hadoopConf = spark.sessionState.newHadoopConf()
      // scalastyle:on deltahadoopconfiguration
      val hadoopConfBroadcast = spark.sparkContext.broadcast(
        new SerializableConfiguration(hadoopConf))

      val metadataDir = s"$tablePath/metadata"
      new java.io.File(metadataDir).mkdirs()

      val ctx = ManifestWriteContext(
        tablePath = tablePath,
        partitionSpec = PartitionSpec.unpartitioned(),
        logicalToPhysicalPartitionNames = Map.empty,
        statsSchema = snapshot.statsSchema,
        tableSchema = snapshot.schema,
        sessionLocalTimeZone = spark.sessionState.conf.sessionLocalTimeZone,
        tableVersion = snapshot.version,
        metadataOutputLocation = metadataDir,
        formatVersion = 2,
        // Use a very small target size to force multiple manifests per partition
        targetManifestSizeBytes = 1L,
        hadoopConfBroadcast = hadoopConfBroadcast
      )

      val manifests = DistributedManifestWriter.writeManifestsDistributed(
        spark, snapshot.allFiles, ctx, numPartitions = 3)

      // With targetManifestSizeBytes=1 and many files, should produce many manifests
      assert(manifests.size > 3,
        s"Expected more than 3 manifests with tiny target size, got ${manifests.size}")

      // Read back all entries and verify total file count
      val allDataFiles = manifests.flatMap { m =>
        readDataFilesFromManifest(m, PartitionSpec.unpartitioned(), hadoopConf)
      }
      assert(allDataFiles.size === snapshot.numOfFiles.toInt)

      // Verify total record count
      val totalRecords = allDataFiles.map(_.recordCount()).sum
      assert(totalRecords === 200L, s"Expected 200 total records, got $totalRecords")
    }
  }

  test("cleanupManifests removes files from disk") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      spark.range(10).toDF("id")
        .write.format("delta").save(tablePath)

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      val snapshot = deltaLog.update()

      // scalastyle:off deltahadoopconfiguration
      val hadoopConf = spark.sessionState.newHadoopConf()
      // scalastyle:on deltahadoopconfiguration
      val hadoopConfBroadcast = spark.sparkContext.broadcast(
        new SerializableConfiguration(hadoopConf))

      val metadataDir = s"$tablePath/metadata"
      new java.io.File(metadataDir).mkdirs()

      val ctx = ManifestWriteContext(
        tablePath = tablePath,
        partitionSpec = PartitionSpec.unpartitioned(),
        logicalToPhysicalPartitionNames = Map.empty,
        statsSchema = snapshot.statsSchema,
        tableSchema = snapshot.schema,
        sessionLocalTimeZone = spark.sessionState.conf.sessionLocalTimeZone,
        tableVersion = snapshot.version,
        metadataOutputLocation = metadataDir,
        formatVersion = 2,
        targetManifestSizeBytes = DistributedManifestWriter.DEFAULT_TARGET_MANIFEST_SIZE_BYTES,
        hadoopConfBroadcast = hadoopConfBroadcast
      )

      val manifests = DistributedManifestWriter.writeManifestsDistributed(
        spark, snapshot.allFiles, ctx, numPartitions = 1)

      assert(manifests.nonEmpty)
      // Verify files exist
      manifests.foreach { m =>
        val f = new java.io.File(m.path().replaceFirst("file:", ""))
        assert(f.exists(), s"Manifest should exist before cleanup: ${m.path()}")
      }

      // Clean up
      DistributedManifestWriter.cleanupManifests(manifests, hadoopConf)

      // Verify files are deleted
      manifests.foreach { m =>
        val f = new java.io.File(m.path().replaceFirst("file:", ""))
        assert(!f.exists(), s"Manifest should be deleted after cleanup: ${m.path()}")
      }
    }
  }

  // ===== Option C: Batched driver path integration tests =====

  test("batched driver path produces identical DataFile metadata to distributed path") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      spark.range(0, 50, 1, 4).toDF("id")
        .write.format("delta").save(tablePath)

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      val snapshot = deltaLog.update()
      val dataPath = new Path(tablePath)

      // scalastyle:off deltahadoopconfiguration
      val hadoopConf = spark.sessionState.newHadoopConf()
      // scalastyle:on deltahadoopconfiguration
      val hadoopConfBroadcast = spark.sparkContext.broadcast(
        new SerializableConfiguration(hadoopConf))

      val metadataDir = s"$tablePath/metadata"
      new java.io.File(metadataDir).mkdirs()

      val partitionSpec = PartitionSpec.unpartitioned()

      val ctx = ManifestWriteContext(
        tablePath = tablePath,
        partitionSpec = partitionSpec,
        logicalToPhysicalPartitionNames = Map.empty,
        statsSchema = snapshot.statsSchema,
        tableSchema = snapshot.schema,
        sessionLocalTimeZone = spark.sessionState.conf.sessionLocalTimeZone,
        tableVersion = snapshot.version,
        metadataOutputLocation = metadataDir,
        formatVersion = 2,
        targetManifestSizeBytes = DistributedManifestWriter.DEFAULT_TARGET_MANIFEST_SIZE_BYTES,
        hadoopConfBroadcast = hadoopConfBroadcast
      )

      // === Batched driver path (Option C) ===
      val batchedManifests = DistributedManifestWriter
        .writeManifestsForPartition(
          snapshot.allFiles.toLocalIterator().asScala, ctx)
        .toSeq

      // === Distributed path (Option B) ===
      val distributedManifests = DistributedManifestWriter.writeManifestsDistributed(
        spark, snapshot.allFiles, ctx, numPartitions = 2)

      // Read back DataFiles from both paths
      val batchedResults = batchedManifests.flatMap { m =>
        readDataFilesFromManifest(m, partitionSpec, hadoopConf)
      }
      val distributedResults = distributedManifests.flatMap { m =>
        readDataFilesFromManifest(m, partitionSpec, hadoopConf)
      }

      assert(batchedResults.size === distributedResults.size,
        s"File count mismatch: batched=${batchedResults.size}, " +
          s"distributed=${distributedResults.size}")

      val batchedByPath = batchedResults.map(df => df.path().toString -> df).toMap
      val distributedByPath = distributedResults.map(df => df.path().toString -> df).toMap

      assert(batchedByPath.keySet === distributedByPath.keySet,
        "Batched and distributed paths should reference the same files")

      batchedByPath.foreach { case (path, batchedDf) =>
        val distDf = distributedByPath(path)
        assert(batchedDf.fileSizeInBytes() === distDf.fileSizeInBytes(),
          s"Size mismatch for $path")
        assert(batchedDf.recordCount() === distDf.recordCount(),
          s"Record count mismatch for $path")
        assert(batchedDf.format() === distDf.format(),
          s"Format mismatch for $path")
      }
    }
  }

  test("batched driver and driver paths produce identical results for partitioned table") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      spark.createDataFrame(Seq(
        (1, "A", 10.0), (2, "A", 20.0), (3, "B", 30.0),
        (4, "B", 40.0), (5, "C", 50.0), (6, "C", 60.0)
      )).toDF("id", "category", "value")
        .write.format("delta").partitionBy("category").save(tablePath)

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      val snapshot = deltaLog.update()
      val allFilesCollected = snapshot.allFiles.collect().toSeq
      val dataPath = new Path(tablePath)

      // scalastyle:off deltahadoopconfiguration
      val hadoopConf = spark.sessionState.newHadoopConf()
      // scalastyle:on deltahadoopconfiguration
      val hadoopConfBroadcast = spark.sparkContext.broadcast(
        new SerializableConfiguration(hadoopConf))

      val metadataDir = s"$tablePath/metadata"
      new java.io.File(metadataDir).mkdirs()

      val icebergSchema = new IcebergSchema(
        NestedField.required(1, "id", IcebergIntType.get()),
        NestedField.required(2, "category", IcebergStringType.get()),
        NestedField.required(3, "value", IcebergDoubleType.get())
      )
      val partitionSpec = IcebergTransactionUtils.createPartitionSpec(
        icebergSchema, Seq("category"))
      val logicalToPhysical = IcebergTransactionUtils.getPartitionPhysicalNameMapping(
        snapshot.metadata.partitionSchema)

      val statsParser = DeltaFileProviderUtils.createJsonStatsParser(
        snapshot.statsSchema, spark.sessionState.conf.sessionLocalTimeZone)

      // === Legacy driver path ===
      val (driverManifest, _) = writeManifestViaDriverPath(
        allFilesCollected, dataPath, partitionSpec, logicalToPhysical,
        DriverWriteParams(statsParser, snapshot.schema, snapshot.version,
          snapshot.statsSchema, metadataDir, 2, hadoopConf))

      // === Batched driver path (Option C) ===
      val ctx = ManifestWriteContext(
        tablePath = tablePath,
        partitionSpec = partitionSpec,
        logicalToPhysicalPartitionNames = logicalToPhysical,
        statsSchema = snapshot.statsSchema,
        tableSchema = snapshot.schema,
        sessionLocalTimeZone = spark.sessionState.conf.sessionLocalTimeZone,
        tableVersion = snapshot.version,
        metadataOutputLocation = metadataDir,
        formatVersion = 2,
        targetManifestSizeBytes = DistributedManifestWriter.DEFAULT_TARGET_MANIFEST_SIZE_BYTES,
        hadoopConfBroadcast = hadoopConfBroadcast
      )

      val batchedManifests = DistributedManifestWriter
        .writeManifestsForPartition(
          snapshot.allFiles.toLocalIterator().asScala, ctx)
        .toSeq

      // Read back
      val driverResults = readDataFilesFromManifest(driverManifest, partitionSpec, hadoopConf)
      val batchedResults = batchedManifests.flatMap { m =>
        readDataFilesFromManifest(m, partitionSpec, hadoopConf)
      }

      assert(driverResults.size === batchedResults.size)

      val driverByPath = driverResults.map(df => df.path().toString -> df).toMap
      val batchedByPath = batchedResults.map(df => df.path().toString -> df).toMap

      assert(driverByPath.keySet === batchedByPath.keySet)

      driverByPath.foreach { case (path, driverDf) =>
        val batchedDf = batchedByPath(path)
        assert(driverDf.fileSizeInBytes() === batchedDf.fileSizeInBytes(),
          s"Size mismatch for $path")
        assert(driverDf.recordCount() === batchedDf.recordCount(),
          s"Record count mismatch for $path")

        val driverPart = driverDf.partition()
        val batchedPart = batchedDf.partition()
        for (i <- 0 until driverPart.size()) {
          assert(
            driverPart.get(i, classOf[AnyRef]) === batchedPart.get(i, classOf[AnyRef]),
            s"Partition value mismatch at index $i for $path")
        }
      }

      // Verify partition values are correct
      val partitionValues = batchedResults.map { df =>
        df.partition().get(0, classOf[String])
      }.toSet
      assert(partitionValues === Set("A", "B", "C"))
    }
  }

  test("all three paths produce identical results: legacy driver, batched driver, distributed") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      spark.range(0, 30, 1, 3).toDF("id")
        .write.format("delta").save(tablePath)

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      val snapshot = deltaLog.update()
      val allFilesCollected = snapshot.allFiles.collect().toSeq
      val dataPath = new Path(tablePath)

      // scalastyle:off deltahadoopconfiguration
      val hadoopConf = spark.sessionState.newHadoopConf()
      // scalastyle:on deltahadoopconfiguration
      val hadoopConfBroadcast = spark.sparkContext.broadcast(
        new SerializableConfiguration(hadoopConf))

      val metadataDir = s"$tablePath/metadata"
      new java.io.File(metadataDir).mkdirs()

      val partitionSpec = PartitionSpec.unpartitioned()
      val statsParser = DeltaFileProviderUtils.createJsonStatsParser(
        snapshot.statsSchema, spark.sessionState.conf.sessionLocalTimeZone)

      // === Path 1: Legacy driver (manual manifest write) ===
      val (legacyManifest, _) = writeManifestViaDriverPath(
        allFilesCollected, dataPath, partitionSpec, Map.empty,
        DriverWriteParams(statsParser, snapshot.schema, snapshot.version,
          snapshot.statsSchema, metadataDir, 2, hadoopConf))

      val ctx = ManifestWriteContext(
        tablePath = tablePath,
        partitionSpec = partitionSpec,
        logicalToPhysicalPartitionNames = Map.empty,
        statsSchema = snapshot.statsSchema,
        tableSchema = snapshot.schema,
        sessionLocalTimeZone = spark.sessionState.conf.sessionLocalTimeZone,
        tableVersion = snapshot.version,
        metadataOutputLocation = metadataDir,
        formatVersion = 2,
        targetManifestSizeBytes = DistributedManifestWriter.DEFAULT_TARGET_MANIFEST_SIZE_BYTES,
        hadoopConfBroadcast = hadoopConfBroadcast
      )

      // === Path 2: Batched driver (Option C) ===
      val batchedManifests = DistributedManifestWriter
        .writeManifestsForPartition(
          snapshot.allFiles.toLocalIterator().asScala, ctx)
        .toSeq

      // === Path 3: Distributed (Option B) ===
      val distributedManifests = DistributedManifestWriter.writeManifestsDistributed(
        spark, snapshot.allFiles, ctx, numPartitions = 2)

      // Read back all DataFiles from each path
      val legacyFiles = readDataFilesFromManifest(legacyManifest, partitionSpec, hadoopConf)
      val batchedFiles = batchedManifests.flatMap(m =>
        readDataFilesFromManifest(m, partitionSpec, hadoopConf))
      val distributedFiles = distributedManifests.flatMap(m =>
        readDataFilesFromManifest(m, partitionSpec, hadoopConf))

      // All three should have the same number of files
      assert(legacyFiles.size === batchedFiles.size)
      assert(legacyFiles.size === distributedFiles.size)

      val legacyByPath = legacyFiles.map(df => df.path().toString -> df).toMap
      val batchedByPath = batchedFiles.map(df => df.path().toString -> df).toMap
      val distributedByPath = distributedFiles.map(df => df.path().toString -> df).toMap

      // All three should reference the same file paths
      assert(legacyByPath.keySet === batchedByPath.keySet)
      assert(legacyByPath.keySet === distributedByPath.keySet)

      // Compare field-by-field across all three paths
      legacyByPath.foreach { case (path, legacyDf) =>
        val batchedDf = batchedByPath(path)
        val distDf = distributedByPath(path)

        Seq(("batched", batchedDf), ("distributed", distDf)).foreach { case (name, df) =>
          assert(legacyDf.fileSizeInBytes() === df.fileSizeInBytes(),
            s"Size mismatch: legacy vs $name for $path")
          assert(legacyDf.recordCount() === df.recordCount(),
            s"Record count mismatch: legacy vs $name for $path")
          assert(legacyDf.format() === df.format(),
            s"Format mismatch: legacy vs $name for $path")
        }
      }

      // Total record counts
      val legacyTotal = legacyFiles.map(_.recordCount()).sum
      val batchedTotal = batchedFiles.map(_.recordCount()).sum
      val distributedTotal = distributedFiles.map(_.recordCount()).sum
      assert(legacyTotal === 30L)
      assert(batchedTotal === 30L)
      assert(distributedTotal === 30L)
    }
  }
}
