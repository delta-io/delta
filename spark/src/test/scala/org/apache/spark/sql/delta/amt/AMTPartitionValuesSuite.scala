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

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType}

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

  /**
   * Asserts the persisted `partition` field for column `name` has exactly the `expected` Iceberg
   * type and the string form of its value.
   */
  private def assertPartitionField(
      persisted: DataFrame,
      name: String,
      expected: (DataType, String)): Unit = {
    val field = persisted.schema("partition").dataType.asInstanceOf[StructType](name)
    val row = persisted.select(s"partition.$name").head()
    val actual = (field.dataType, String.valueOf(row.get(0)))
    assert(actual == expected, s"partition.$name\n  expected=$expected\n  actual=$actual")
  }

  test("forWrite persists partition values as the typed struct, keyed by logical name") {
    withAllTypesTable("amt_partition_write", numFiles = 1) { deltaLog =>
      val snapshot = deltaLog.update()
      val partitionSchema = snapshot.metadata.partitionSchema
      val add = liveAddFiles(snapshot).head
      val entry = DataEntry.fromAddFile(
        add, AMTWriteHelper.addedTrackingForDataEntry(), deltaLog.dataPath)
      val input = Seq(entry.wrap).toDS().toDF()
      val persisted = AMTPartitionValues.forWrite(input, partitionSchema)

      // The struct keeps the partition columns' *logical* names, though the source map is keyed by
      // physical name -- an Iceberg reader resolves them by field id, so the name is informational.
      val partition = persisted.schema("partition").dataType.asInstanceOf[StructType]
      assert(partition.fieldNames.toSeq == partitionableTestColumns.map("p_" + _.name))

      assertPartitionField(persisted, "p_int", (IntegerType, "0"))
      assertPartitionField(persisted, "p_long", (LongType, "0"))
      assertPartitionField(persisted, "p_short", (ShortType, "0"))
      assertPartitionField(persisted, "p_byte", (ByteType, "0"))
      assertPartitionField(persisted, "p_str", (StringType, "row0"))
      assertPartitionField(persisted, "p_date", (DateType, "2026-07-25"))
      assertPartitionField(persisted, "p_ts", (TimestampType, "2026-07-25 01:02:03.456"))
      assertPartitionField(persisted, "p_ts_ntz", (TimestampNTZType, "2026-07-25T01:02:03.456"))
      assertPartitionField(persisted, "p_dec", (DecimalType(9, 3), "0.000"))
      assertPartitionField(persisted, "p_bool", (BooleanType, "false"))
      assertPartitionField(persisted, "p_float", (FloatType, "0.0"))
      assertPartitionField(persisted, "p_double", (DoubleType, "0.0"))
      // Binary's string form is not stable, so assert only that it keeps its type.
      assert(partition("p_binary").dataType == BinaryType,
        s"p_binary should stay binary: ${partition("p_binary")}")
    }
  }

  test("forRead reproduces the partition values the delta log holds, for every type") {
    withAllTypesTable("amt_partition_roundtrip", numFiles = leafPackedFiles) { deltaLog =>
      commitCheckpoint(deltaLog, incremental = false)
      val snapshot = deltaLog.update()
      val provider = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))
      val partitionSchema = snapshot.metadata.partitionSchema
      val leaves = provider.liveLeafManifestAbsolutePaths.map(_.toString)
      assert(leaves.nonEmpty, "Expected at least one leaf manifest.")

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
      assert(logged.size == leafPackedFiles,
        s"Expected one logged add per file, got ${logged.size}.")

      // forRead reconstructs the physical-name partition-value map from the leaves' DATA entries.
      val reconstructed = withManifestDataEntries(leaves) { entries =>
        AMTPartitionValues.forRead(entries, partitionSchema)
          .select(col("partition"))
          .collect()
          .map(_.getMap[String, String](0).toMap)
          .toSet
      }
      assert(reconstructed == logged,
        s"forRead did not reproduce the logged partition values.\n" +
          s"  logged=$logged\n  reconstructed=$reconstructed")
    }
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
}
