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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DecimalType, IntegerType, LongType, MapType, MetadataBuilder, StringType, StructField, StructType, TimestampType}

/**
 * Tests for [[AMTPartitionValues]], the converter between Delta's physical-name partition string
 * map and the typed Iceberg partition struct a manifest persists. Covers the conversion in
 * isolation, on hand-built entries, and end to end against a real checkpoint's leaves.
 */
class AMTPartitionValuesSuite extends AMTCheckpointTestBase {

  import testImplicits._

  /** A stand-in table root; the entries here carry no DV, so it is only a path placeholder. */
  private val tableRoot = new Path("file:/tmp/amt-test-table")

  /** A minimal leaf tracking envelope (ADDED status). */
  private def addedTracking: Tracking = Tracking(
    status = Tracking.Status.Added,
    snapshot_id = None,
    dv_snapshot_id = None,
    sequence_number = None,
    file_sequence_number = None,
    first_row_id = None,
    deleted_positions = None,
    replaced_positions = None)

  private def sampleAddFile: AddFile = AddFile(
    path = "part-00000.parquet",
    partitionValues = Map("p" -> "1"),
    size = 1024L,
    modificationTime = 100L,
    dataChange = true,
    stats = """{"numRecords":42}""")

  /** A DataFrame of one wrapped DATA entry carrying `partitionValues`. */
  private def entryWith(partitionValues: Map[String, String]) =
    Seq(sampleAddFile.copy(partitionValues = partitionValues))
      .map(DataEntry.fromAddFile(_, addedTracking, tableRoot).wrap).toDS().toDF()

  /** A partition column whose physical name differs from its logical one, as column mapping
   *  assigns them. */
  private def partitionColumn(logical: String, physical: String, dataType: DataType) =
    StructField(logical, dataType, nullable = true,
      new MetadataBuilder()
        .putString(DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, physical)
        .build())

  test("partition adapter writes typed logical values and restores the physical-name map") {
    val partitionSchema = StructType(Seq(
      partitionColumn("region", "col-1", IntegerType),
      partitionColumn("day", "col-2", DateType),
      partitionColumn("name", "col-3", StringType),
      partitionColumn("count", "col-4", LongType),
      partitionColumn("active", "col-5", BooleanType),
      partitionColumn("amount", "col-6", DecimalType(9, 3)),
      partitionColumn("at", "col-7", TimestampType)))
    // The values a Delta log would hold for these columns. `col-7` is already UTC-normalized
    // because that is the form `literalToNormalizedString` writes a timestamp in; the round trip
    // below is only lossless for the form the writer actually produces.
    val logged = Map(
      "col-1" -> "7",
      "col-2" -> "2026-07-25",
      "col-3" -> "us-west-2",
      "col-4" -> "9007199254740993",
      "col-5" -> "true",
      "col-6" -> "1.250",
      "col-7" -> "2026-07-25T08:02:03.456000Z")
    val input = entryWith(logged)

    // The persisted struct is keyed by *logical* name and typed, while the source map was keyed by
    // physical name with string values.
    val persistedDF = AMTPartitionValues.forWrite(input, partitionSchema)
    assert(persistedDF.schema("partition").dataType.asInstanceOf[StructType]
      .map(f => f.name -> f.dataType) == partitionSchema.map(f => f.name -> f.dataType))

    val partition = persistedDF.select("partition.*").head()
    assert(partition.getInt(0) == 7)
    assert(partition.getDate(1).toString == "2026-07-25")
    assert(partition.getString(2) == "us-west-2")
    // Wider than a Double can hold exactly, so this also shows the value is not going through one.
    assert(partition.getLong(3) == 9007199254740993L)
    assert(partition.getBoolean(4))
    assert(partition.getDecimal(5) == new java.math.BigDecimal("1.250"))
    assert(partition.getTimestamp(6).toInstant.toString == "2026-07-25T08:02:03.456Z")

    // The whole struct collects as a nested Row of typed values, not as a map of strings.
    val persistedRow = persistedDF.select("partition").collect().head.getStruct(0)
    assert(persistedRow.schema.fieldNames.toSeq == partitionSchema.map(_.name))
    assert(persistedRow.getInt(0) == 7)

    // Reading it back restores exactly the physical-name string map the Delta log held.
    val restoredDF = AMTPartitionValues.forRead(persistedDF, partitionSchema)
    assert(restoredDF.select("partition").collect().toSeq == Seq(Row(logged)))
    val restored = restoredDF.select(col("partition")).as[Map[String, String]].head()
    assert(restored == logged)
  }

  test("partition adapter rejects an invalid non-null typed value") {
    val schema = StructType(Seq(StructField("p", IntegerType)))
    val input = entryWith(Map("p" -> "not-an-int"))

    // `forWrite` casts with ANSI on, so an unparseable value fails the write rather than silently
    // persisting a null partition value.
    val error = intercept[Exception] {
      AMTPartitionValues.forWrite(input, schema).collect()
    }
    assert(error.toString.contains("CAST_INVALID_INPUT"))
  }

  /**
   * The physical-name partition-value maps `forRead` reconstructs from the manifest tree's DATA
   * entries.
   */
  private def reconstructPartitionValues(
      manifests: Seq[String],
      partitionSchema: StructType): Set[Map[String, String]] =
    allowReadWithinDeltaLog {
      val entries = spark.read.parquet(manifests: _*)
        .where(col("content_type") === AMTSingleAction.ContentType.Type.Data)
      AMTPartitionValues.forRead(entries, partitionSchema)
        .select(col("partition"))
        .collect()
        .map(_.getMap[String, String](0).toMap)
        .toSet
    }

  test("forRead reproduces the partition values the delta log holds, for every type") {
    val numFiles = 4
    withAllPartitionTypesTable(
        "amt_partition_roundtrip", numFiles = numFiles, maxEntriesPerLeaf = 2) { deltaLog =>
      commitCheckpoint(deltaLog, incremental = false)
      val snapshot = deltaLog.update()
      val provider = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))
      val partitionSchema = snapshot.metadata.partitionSchema
      // A full rewrite can hash every file into one Spark partition. In that valid representation,
      // the sole leaf is promoted to the root and there are no leaf pointers to read.
      val manifests = provider.topLevelFiles.map(_.getPath.toString) ++
        provider.liveLeafManifestAbsolutePaths.map(_.toString)
      assert(manifests.nonEmpty, "Expected at least a root manifest.")

      // Read the commit json directly rather than going through the snapshot: an AMT snapshot
      // reconstructs its AddFiles through `forRead`, so comparing against it would compare
      // `forRead` with itself and hold even if the conversion were wrong.
      val loggedSchema = StructType(Seq(StructField("add", StructType(Seq(
        StructField("partitionValues", MapType(StringType, StringType)))))))
      val logged = spark.read
        .schema(loggedSchema)
        .json(new Path(deltaLog.logPath, "*.json").toString)
        .where(col("add").isNotNull)
        .select(col("add.partitionValues"))
        .collect()
        .map(_.getMap[String, String](0).toMap)
        .toSet
      assert(logged.size == numFiles, s"Expected one logged add per file, got ${logged.size}.")

      val reconstructed = reconstructPartitionValues(manifests, partitionSchema)
      assert(reconstructed == logged,
        s"forRead did not reproduce the logged partition values.\n" +
          s"  logged=$logged\n  reconstructed=$reconstructed")
    }
  }
}
