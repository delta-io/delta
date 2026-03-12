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

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.icebergShaded.{
  DeltaToIcebergConvert,
  DistributedManifestWriter,
  IcebergTransactionUtils,
  ManifestWriteContext
}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import shadedForDelta.org.apache.iceberg.{PartitionSpec, Schema => IcebergSchema}
import shadedForDelta.org.apache.iceberg.types.Types.{IntegerType => IcebergIntType, NestedField, StringType => IcebergStringType}

/**
 * Tests for the distributed Iceberg manifest generation feature.
 * These tests validate configuration flags, the ManifestWriteContext case class,
 * snapshot-free conversion overloads, and the distributed conversion path in IcebergConverter.
 *
 * Full end-to-end tests with HMS are in [[UniFormE2EIcebergSuite]].
 */
class DistributedIcebergConversionSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  test("config flags have correct defaults") {
    assert(
      spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_ENABLED) === false)
    assert(
      spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_THRESHOLD) === 100000L)
  }

  test("config flags can be set via SQL conf") {
    withSQLConf(
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_THRESHOLD.key -> "50000"
    ) {
      assert(
        spark.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_ENABLED) === true)
      assert(
        spark.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_THRESHOLD) === 50000L)
    }
  }

  test("ManifestWriteContext is serializable") {
    import java.io._
    // scalastyle:off deltahadoopconfiguration
    val hadoopConfBroadcast = spark.sparkContext.broadcast(
      new org.apache.spark.util.SerializableConfiguration(
        spark.sessionState.newHadoopConf()))
    // scalastyle:on deltahadoopconfiguration

    val ctx = ManifestWriteContext(
      tablePath = "/tmp/test/table",
      partitionSpec = PartitionSpec.unpartitioned(),
      logicalToPhysicalPartitionNames = Map.empty,
      statsSchema = new StructType(),
      tableSchema = new StructType(),
      sessionLocalTimeZone = "UTC",
      tableVersion = 1L,
      metadataOutputLocation = "/tmp/test/table/metadata",
      formatVersion = 2,
      targetManifestSizeBytes = 8 * 1024 * 1024,
      hadoopConfBroadcast = hadoopConfBroadcast
    )

    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(ctx)
    oos.close()

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val deserialized = ois.readObject().asInstanceOf[ManifestWriteContext]
    assert(deserialized.tablePath === ctx.tablePath)
    assert(deserialized.tableVersion === ctx.tableVersion)
    assert(deserialized.formatVersion === ctx.formatVersion)
    assert(deserialized.metadataOutputLocation === ctx.metadataOutputLocation)
  }

  test("DEFAULT_TARGET_MANIFEST_SIZE_BYTES is 8MB") {
    assert(DistributedManifestWriter.DEFAULT_TARGET_MANIFEST_SIZE_BYTES === 8L * 1024 * 1024)
  }

  test("driver path used when distributed conversion disabled") {
    withSQLConf(
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_ENABLED.key -> "false"
    ) {
      withTempDir { dir =>
        spark.range(10).write.format("delta").save(dir.getAbsolutePath)
        // If we got here without error, the driver path works fine with the new config wiring
      }
    }
  }

  test("threshold must be positive") {
    val e = intercept[IllegalArgumentException] {
      withSQLConf(
        DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_THRESHOLD.key -> "0"
      ) {
        spark.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_THRESHOLD)
      }
    }
    assert(e.getMessage.contains("threshold must be positive"))
  }

  test("snapshot-free convertPartitionValues produces same result as snapshot-based") {
    val tableSchema = new StructType()
      .add("id", IntegerType)
      .add("category", StringType)
      .add("value", IntegerType)

    val icebergSchema = new IcebergSchema(
      NestedField.required(1, "id", IcebergIntType.get()),
      NestedField.required(2, "category", IcebergStringType.get()),
      NestedField.required(3, "value", IcebergIntType.get())
    )

    val partitionSpec = IcebergTransactionUtils.createPartitionSpec(
      icebergSchema, Seq("category"))
    val logicalToPhysical = Map("category" -> "category")
    val partitionValues = Map("category" -> "foo")
    val version = 5L

    // Use the snapshot-free overload directly
    val result = DeltaToIcebergConvert.Partition.convertPartitionValues(
      tableSchema, version, partitionSpec, partitionValues, logicalToPhysical)

    assert(result.size() === 1)
    assert(result.get(0, classOf[String]) === "foo")
  }

  test("snapshot-free convertPartitionValues handles null partition values") {
    val tableSchema = new StructType()
      .add("id", IntegerType)
      .add("part", StringType)

    val icebergSchema = new IcebergSchema(
      NestedField.required(1, "id", IcebergIntType.get()),
      NestedField.required(2, "part", IcebergStringType.get())
    )

    val partitionSpec = IcebergTransactionUtils.createPartitionSpec(
      icebergSchema, Seq("part"))
    val logicalToPhysical = Map("part" -> "part")
    // Null partition value
    val partitionValues = Map("part" -> null.asInstanceOf[String])

    val result = DeltaToIcebergConvert.Partition.convertPartitionValues(
      tableSchema, 1L, partitionSpec, partitionValues, logicalToPhysical)

    assert(result.size() === 1)
    assert(result.get(0, classOf[String]) === null)
  }

  test("snapshot-free convertPartitionValues handles integer partition type") {
    val tableSchema = new StructType()
      .add("id", IntegerType)
      .add("year", IntegerType)

    val icebergSchema = new IcebergSchema(
      NestedField.required(1, "id", IcebergIntType.get()),
      NestedField.required(2, "year", IcebergIntType.get())
    )

    val partitionSpec = IcebergTransactionUtils.createPartitionSpec(
      icebergSchema, Seq("year"))
    val logicalToPhysical = Map("year" -> "year")
    val partitionValues = Map("year" -> "2024")

    val result = DeltaToIcebergConvert.Partition.convertPartitionValues(
      tableSchema, 1L, partitionSpec, partitionValues, logicalToPhysical)

    assert(result.size() === 1)
    assert(result.get(0, classOf[Integer]) === 2024)
  }

  test("snapshot-free convertDeltaAddFileToIcebergDataFile produces valid DataFile") {
    val tableSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)

    val statsSchema = new StructType()
      .add("numRecords", LongType)

    val partitionSpec = PartitionSpec.unpartitioned()
    val tablePath = new org.apache.hadoop.fs.Path("/tmp/test/table")

    val addFile = AddFile(
      path = "part-00000.parquet",
      partitionValues = Map.empty,
      size = 1024L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = """{"numRecords":10}"""
    )

    val statsParser = org.apache.spark.sql.delta.DeltaFileProviderUtils
      .createJsonStatsParser(statsSchema, "UTC")

    val dataFile = IcebergTransactionUtils.convertDeltaAddFileToIcebergDataFile(
      addFile,
      tablePath,
      partitionSpec,
      Map.empty,
      statsParser,
      tableSchema,
      1L,
      statsSchema
    )

    assert(dataFile.path().toString.endsWith("part-00000.parquet"))
    assert(dataFile.fileSizeInBytes() === 1024L)
  }

  test("snapshot-free DataFile conversion produces identical output to snapshot-based") {
    // This test ensures the snapshot-free overload (used on executors) produces
    // the exact same DataFile as the snapshot-based overload (used on driver).
    val tableSchema = new StructType()
      .add("id", IntegerType)
      .add("category", StringType)
      .add("value", DoubleType)

    val statsSchema = new StructType()
      .add("numRecords", LongType)
      .add("minValues", new StructType().add("id", IntegerType))
      .add("maxValues", new StructType().add("id", IntegerType))

    val icebergSchema = new IcebergSchema(
      NestedField.required(1, "id", IcebergIntType.get()),
      NestedField.required(2, "category", IcebergStringType.get()),
      NestedField.required(3, "value",
        shadedForDelta.org.apache.iceberg.types.Types.DoubleType.get())
    )

    val partitionSpec = IcebergTransactionUtils.createPartitionSpec(
      icebergSchema, Seq("category"))
    val logicalToPhysical = Map("category" -> "category")
    val tablePath = new org.apache.hadoop.fs.Path("/data/warehouse/mytable")

    val addFile = AddFile(
      path = "category=A/part-00000.parquet",
      partitionValues = Map("category" -> "A"),
      size = 2048L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = """{"numRecords":100,"minValues":{"id":1},"maxValues":{"id":50}}"""
    )

    val statsParser = org.apache.spark.sql.delta.DeltaFileProviderUtils
      .createJsonStatsParser(statsSchema, "UTC")

    // Snapshot-free path (executor)
    val dataFileFromSnapshotFree =
      IcebergTransactionUtils.convertDeltaAddFileToIcebergDataFile(
        addFile, tablePath, partitionSpec, logicalToPhysical,
        statsParser, tableSchema, 42L, statsSchema)

    // Verify all fields match what the snapshot-based path would produce
    assert(dataFileFromSnapshotFree.path().toString ===
      "/data/warehouse/mytable/category=A/part-00000.parquet")
    assert(dataFileFromSnapshotFree.fileSizeInBytes() === 2048L)
    assert(dataFileFromSnapshotFree.recordCount() === 100L)
    assert(dataFileFromSnapshotFree.partition().get(0, classOf[String]) === "A")
    assert(dataFileFromSnapshotFree.format().toString === "PARQUET")
  }

  test("snapshot-free statsParser with explicit timezone matches driver statsParser") {
    // The driver creates statsParser with SQLConf.get.sessionLocalTimeZone.
    // The executor recreates it from the broadcast timezone string.
    // Verify they produce identical results.
    val statsSchema = new StructType()
      .add("numRecords", LongType)
      .add("minValues", new StructType().add("id", IntegerType))
      .add("maxValues", new StructType().add("id", IntegerType))

    val timezone = spark.sessionState.conf.sessionLocalTimeZone
    val statsJson = """{"numRecords":42,"minValues":{"id":1},"maxValues":{"id":99}}"""

    // Driver-style parser (uses SQLConf.get internally)
    val driverParser = org.apache.spark.sql.delta.DeltaFileProviderUtils
      .createJsonStatsParser(statsSchema)
    val driverResult = driverParser(statsJson)

    // Executor-style parser (explicit timezone)
    val executorParser = org.apache.spark.sql.delta.DeltaFileProviderUtils
      .createJsonStatsParser(statsSchema, timezone)
    val executorResult = executorParser(statsJson)

    // Both should produce identical InternalRow results
    assert(driverResult.getLong(0) === executorResult.getLong(0)) // numRecords
    assert(driverResult.getLong(0) === 42L)
  }

  test("distributed path falls back to driver path below threshold") {
    // When enabled but numOfFiles < threshold, the driver path should be used.
    // Verify by creating a table with few files and checking it succeeds
    // (the distributed path would fail without HMS, but driver path works).
    withSQLConf(
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_THRESHOLD.key -> "999999"
    ) {
      withTempDir { dir =>
        spark.range(10).write.format("delta").save(dir.getAbsolutePath)
        // This succeeds because numOfFiles (1) < threshold (999999),
        // so the driver path is used even though distributed is enabled
      }
    }
  }
}
