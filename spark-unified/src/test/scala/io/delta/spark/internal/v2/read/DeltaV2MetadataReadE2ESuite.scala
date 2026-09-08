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

package io.delta.spark.internal.v2.read

import java.net.URI
import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.StructType

private[read] trait DeltaV2MetadataReadE2ETests {
  self: DeltaV2ScanE2ETestUtils =>

  private val baseMetadataFieldNames =
    FileFormat.BASE_METADATA_FIELDS.map(_.name)

  private def withMetadataTable(
      table: String,
      rowTrackingEnabled: Boolean)(body: String => Unit): Unit = {
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (id LONG, name STRING) USING $tableProvider " +
            s"TBLPROPERTIES ('delta.enableRowTracking' = '$rowTrackingEnabled')")
        withSQLConf("spark.sql.leafNodeDefaultParallelism" -> "1") {
          sql(s"INSERT INTO $table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        }
      }
      val tableLocation = withV1Mode {
        sql(s"DESCRIBE DETAIL $table").select("location").head().getString(0)
      }
      body(tableLocation)
    }
  }

  private def assertFileBelongsToTable(filePath: String, tableLocation: String): Unit = {
    val normalizedFileUri = new URI(filePath).normalize()
    val normalizedTableUri = new URI(tableLocation).normalize()
    assert(
      Option(normalizedFileUri.getScheme) == Option(normalizedTableUri.getScheme),
      s"expected file URI scheme to match table location $tableLocation, got $filePath")
    assert(
      Option(normalizedFileUri.getAuthority) == Option(normalizedTableUri.getAuthority),
      s"expected file URI authority to match table location $tableLocation, got $filePath")
    val normalizedTablePath = normalizedTableUri.getPath.stripSuffix("/")
    assert(
      normalizedFileUri.getPath.startsWith(s"$normalizedTablePath/"),
      s"expected file path under table location $tableLocation, got $filePath")
  }

  private def assertMetadataValueShape(
      fieldName: String,
      value: Any,
      tableLocation: String): Unit = {
    assert(value != null, s"expected _metadata.$fieldName to be populated")
    fieldName match {
      case "file_path" =>
        val filePath = value.asInstanceOf[String]
        assert(filePath.startsWith("file:"), s"expected file URI, got $filePath")
        assert(filePath.endsWith(".parquet"), s"expected Parquet path, got $filePath")
        assertFileBelongsToTable(filePath, tableLocation)
      case "file_name" =>
        val fileName = value.asInstanceOf[String]
        assert(fileName.endsWith(".parquet"), s"expected Parquet file name, got $fileName")
        assert(!fileName.contains("/"), s"expected a leaf file name, got $fileName")
      case "file_size" =>
        assert(value.asInstanceOf[Long] > 0L, s"expected positive file size, got $value")
      case "file_block_start" =>
        assert(value.asInstanceOf[Long] >= 0L, s"expected non-negative block start, got $value")
      case "file_block_length" =>
        assert(value.asInstanceOf[Long] > 0L, s"expected positive block length, got $value")
      case "file_modification_time" =>
        val modificationTime = value.asInstanceOf[Timestamp]
        assert(
          modificationTime.getTime > 0L,
          s"expected positive file modification epoch, got $modificationTime")
    }
  }

  private def assertSingleFileMetadataRows(
      df: DataFrame,
      valueOrdinal: Int,
      fieldName: String,
      tableLocation: String): Unit = {
    val rows = df.collect().toSeq
    assert(rows.length == 3, s"expected three rows, got ${rows.length}")
    rows.foreach { row =>
      assertMetadataValueShape(fieldName, row.get(valueOrdinal), tableLocation)
    }
    val values = rows.map(_.get(valueOrdinal))
    assert(
      values.distinct.size == 1,
      s"expected one source file for _metadata.$fieldName, got ${values.distinct}")
  }

  private def assertMetadataStruct(metadata: Row, tableLocation: String): Unit = {
    assert(metadata != null, "expected _metadata to be populated")
    baseMetadataFieldNames.zipWithIndex.foreach { case (fieldName, ordinal) =>
      assertMetadataValueShape(fieldName, metadata.get(ordinal), tableLocation)
    }
  }

  Seq(true, false).foreach { rowTrackingEnabled =>
    test(s"file_path is available when row tracking is $rowTrackingEnabled") {
      val table = s"v2_scan_e2e_metadata_path_$rowTrackingEnabled"
      withMetadataTable(table, rowTrackingEnabled) { tableLocation =>
        val df =
          sql(s"SELECT id, _metadata.file_path FROM $table ORDER BY id")
        assert(df.schema.fieldNames.toSeq == Seq("id", "file_path"))
        assert(df.collect().map(_.getLong(0)).toSeq == Seq(1L, 2L, 3L))
        assertSingleFileMetadataRows(
          df,
          valueOrdinal = 1,
          fieldName = "file_path",
          tableLocation = tableLocation)
        assertExpectedScan(df, s"file_path read with row tracking $rowTrackingEnabled")
      }
    }
  }

  baseMetadataFieldNames.foreach { fieldName =>
    test(s"base metadata field $fieldName has the expected shape") {
      val table = s"v2_scan_e2e_metadata_${fieldName}_field"
      withMetadataTable(table, rowTrackingEnabled = false) { tableLocation =>
        val query =
          s"SELECT id, _metadata.$fieldName AS metadata_value FROM $table ORDER BY id"
        val df = sql(query)
        assert(df.schema.fieldNames.toSeq == Seq("id", "metadata_value"))
        assertSingleFileMetadataRows(
          df,
          valueOrdinal = 1,
          fieldName = fieldName,
          tableLocation = tableLocation)
        if (fieldName == "file_modification_time") {
          val v1Rows = withV1Mode {
            sql(query).collect().toSeq
          }
          checkAnswer(df, v1Rows)
        }
        assertExpectedScan(df, s"$fieldName metadata read")
      }
    }
  }

  test("the whole metadata struct contains every base field") {
    val table = "v2_scan_e2e_metadata_whole"
    withMetadataTable(table, rowTrackingEnabled = false) { tableLocation =>
      val df = sql(s"SELECT _metadata FROM $table")
      assert(df.schema.fieldNames.toSeq == Seq("_metadata"))
      val metadataType = df.schema("_metadata").dataType.asInstanceOf[StructType]
      assert(metadataType.fieldNames.toSeq == baseMetadataFieldNames)
      val rows = df.collect().toSeq
      assert(rows.length == 3, s"expected three rows, got ${rows.length}")
      rows.foreach(row => assertMetadataStruct(row.getStruct(0), tableLocation))
      assertExpectedScan(df, "whole metadata struct read")
    }
  }

  test("metadata alongside star preserves metadata and data columns") {
    val table = "v2_scan_e2e_metadata_star"
    withMetadataTable(table, rowTrackingEnabled = false) { tableLocation =>
      val df = sql(s"SELECT _metadata, * FROM $table ORDER BY id")
      assert(df.schema.fieldNames.toSeq == Seq("_metadata", "id", "name"))
      val rows = df.collect().toSeq
      assert(rows.map(_.getLong(1)) == Seq(1L, 2L, 3L))
      assert(rows.map(_.getString(2)) == Seq("Alice", "Bob", "Charlie"))
      rows.foreach(row => assertMetadataStruct(row.getStruct(0), tableLocation))
      assertExpectedScan(df, "metadata alongside star read")
    }
  }
}

/** Covers Spark base `_metadata` field values and schemas shared across Delta V2 scan paths.
 * Row-tracking-only metadata fields stay outside this suite.
 */
class DeltaV2MetadataReadE2ESuite
  extends DeltaV2ScanE2ETestUtils
  with DeltaV2MetadataReadE2ETests
{
}
